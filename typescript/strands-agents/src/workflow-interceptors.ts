import type { WorkflowInterceptorsFactory } from '@temporalio/workflow';
import { decideCallerFailureConverter } from './failure-converter';

/**
 * Workflow interceptors that {@link StrandsPlugin} adds to the worker: when a run starts, decide
 * whether its workflow code uses the caller's failure converter.
 */
// ts-prune-ignore-next
export const interceptors: WorkflowInterceptorsFactory = () => ({
  inbound: [
    {
      execute(input, next) {
        decideCallerFailureConverter();
        return next(input);
      },
    },
  ],
});
