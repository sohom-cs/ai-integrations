import { ContextWindowOverflowError, MaxTokensError, StructuredOutputError } from '@strands-agents/sdk';
import {
  ApplicationFailure,
  DefaultFailureConverter,
  type FailureConverter,
  type PayloadConverter,
  type ProtoFailure,
  type SerializationContext,
} from '@temporalio/common';
import { inWorkflowContext, patched, workflowInfo } from '@temporalio/workflow';

export const STRANDS_INTERRUPT_TYPE = 'StrandsInterrupt';

// Strands does not re-export `InterruptError` from its public index, so detect
// it structurally instead of via `instanceof`.
interface InterruptLike {
  toJSON(): { id: string; name: string; reason?: unknown; response?: unknown; source: string };
}

function isInterruptError(err: unknown): err is { interrupts: InterruptLike[] } {
  if (err === null || typeof err !== 'object') return false;
  const candidate = err as { interrupts?: unknown };
  if (!Array.isArray(candidate.interrupts) || candidate.interrupts.length === 0) return false;
  const first = candidate.interrupts[0] as { toJSON?: unknown } | undefined;
  return typeof first?.toJSON === 'function';
}

const TERMINAL_ERROR_TYPES = [
  { ctor: ContextWindowOverflowError, name: 'ContextWindowOverflowError' },
  { ctor: MaxTokensError, name: 'MaxTokensError' },
  { ctor: StructuredOutputError, name: 'StructuredOutputError' },
];

function rewriteError(err: unknown): unknown {
  if (isInterruptError(err)) {
    const details = err.interrupts.map((i) => i.toJSON());
    const names = err.interrupts.map((i) => i.toJSON().name).join(',');
    return ApplicationFailure.create({
      message: `interrupt:${names}`,
      type: STRANDS_INTERRUPT_TYPE,
      nonRetryable: true,
      details,
    });
  }
  if (err instanceof Error) {
    for (const { ctor, name } of TERMINAL_ERROR_TYPES) {
      if (err instanceof ctor) {
        return ApplicationFailure.create({
          message: err.message,
          type: name,
          nonRetryable: true,
        });
      }
    }
  }
  return err;
}

/**
 * Failure converter that preserves Strands `InterruptError` payloads across the
 * activity boundary as typed, non-retryable {@link ApplicationFailure}s and
 * marks deterministic Strands model errors as non-retryable.
 *
 * Read by the worker and client via {@link DataConverter.failureConverterPath};
 * the named export below is what {@link StrandsPlugin} wires in.
 */
export class StrandsFailureConverter extends DefaultFailureConverter implements FailureConverter {
  override errorToFailure(
    err: unknown,
    payloadConverter: PayloadConverter,
    context?: SerializationContext
  ): ProtoFailure {
    return super.errorToFailure(rewriteError(err), payloadConverter, context);
  }
}

// ts-prune-ignore-next
export const failureConverter: FailureConverter = new StrandsFailureConverter();

/**
 * Patch that marks the runs in which workflow code decodes failures with the caller's failure
 * converter. Runs recorded before it keep Strands' converter for decoding in workflow code, so they
 * get the same errors on replay.
 */
export const CALLER_FAILURE_CONVERTER_PATCH = 'strands-agents.caller-failure-converter';

// Workflow module state, so one copy per workflow run.
let callerConverter: FailureConverter | undefined;
let decided = false;
let callerDecodesInWorkflow = false;
// The SDK decodes the previous run's failure (`workflowInfo().lastFailure`) while it initializes a
// run, before the decision below can be made.
let earlyDecode:
  | { failure: ProtoFailure; payloadConverter: PayloadConverter; context?: SerializationContext; error: Error }
  | undefined;

/**
 * Decide, when a workflow run starts, whether its workflow code decodes failures with the caller's
 * failure converter. Called by the plugin's workflow interceptor; does nothing without a caller
 * converter. In a run that does, `workflowInfo().lastFailure` is decoded again with the caller's
 * converter, before workflow code reads it.
 */
export function decideCallerFailureConverter(): void {
  if (callerConverter === undefined || decided) return;
  decided = true;
  callerDecodesInWorkflow = patched(CALLER_FAILURE_CONVERTER_PATCH);
  const early = earlyDecode;
  earlyDecode = undefined;
  // `workflowInfo()` returns the run's info object; there is no public setter for `lastFailure`.
  const info = workflowInfo() as unknown as { lastFailure?: Error };
  if (callerDecodesInWorkflow && early !== undefined && info.lastFailure === early.error) {
    info.lastFailure = callerConverter.failureToError(early.failure, early.payloadConverter, early.context);
  }
}

/**
 * Put Strands' rewrite of interrupt and terminal errors in front of the caller's failure converter,
 * so both apply. {@link StrandsPlugin} loads this through a generated module when the client or
 * worker already has a `failureConverterPath`.
 *
 * Encoding only changes what history stores, not what workflow code sees, so it always uses the
 * caller's converter. Decoding in workflow code uses it only in runs that recorded
 * {@link CALLER_FAILURE_CONVERTER_PATCH}; others keep {@link StrandsFailureConverter}.
 */
export function composeFailureConverter(caller: FailureConverter): FailureConverter {
  callerConverter = caller;
  return {
    errorToFailure(err, payloadConverter, context) {
      return caller.errorToFailure(rewriteChain(err), payloadConverter, context);
    },
    failureToError(failure, payloadConverter, context) {
      if (!inWorkflowContext() || callerDecodesInWorkflow) {
        return caller.failureToError(failure, payloadConverter, context);
      }
      const error = failureConverter.failureToError(failure, payloadConverter, context);
      if (!decided) earlyDecode = { failure, payloadConverter, context, error };
      return error;
    },
  };
}

/**
 * Rewrite `err` and every error in its `cause` chain, as {@link StrandsFailureConverter} does through
 * its own recursion, which the caller's converter does not call. `err` itself is not changed: an
 * error whose cause was rewritten is replaced with an object that inherits from it.
 */
function rewriteChain(err: unknown): unknown {
  const rewritten = rewriteError(err);
  if (rewritten !== err || !(err instanceof Error) || err.cause === undefined) return rewritten;
  const cause = rewriteChain(err.cause);
  return cause === err.cause ? err : Object.assign(Object.create(err) as Error, { cause });
}
