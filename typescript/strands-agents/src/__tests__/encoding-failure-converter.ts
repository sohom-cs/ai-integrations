import { ApplicationFailure, DefaultFailureConverter } from '@temporalio/common';
import type { PayloadConverter, ProtoFailure, SerializationContext } from '@temporalio/common';

// A caller's failure converter: failure messages and stack traces go into an encoded payload, and a
// failure of type `Custom` is decoded with its own message, so a test can tell who decoded it.
class EncodingFailureConverter extends DefaultFailureConverter {
  override failureToError(
    failure: ProtoFailure,
    payloadConverter: PayloadConverter,
    context?: SerializationContext
  ): Error {
    const err = super.failureToError(failure, payloadConverter, context);
    return err instanceof ApplicationFailure && err.type === 'Custom'
      ? ApplicationFailure.create({ message: 'decoded by the caller', type: 'Custom' })
      : err;
  }
}

// ts-prune-ignore-next
export const failureConverter = new EncodingFailureConverter({ encodeCommonAttributes: true });
