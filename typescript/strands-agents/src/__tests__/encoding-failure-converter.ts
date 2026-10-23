import { DefaultFailureConverter } from '@temporalio/common';

// A caller's failure converter: failure messages and stack traces go into an encoded payload.
// ts-prune-ignore-next
export const failureConverter = new DefaultFailureConverter({ encodeCommonAttributes: true });
