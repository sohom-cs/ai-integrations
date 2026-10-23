import { proxyActivities, workflowInfo, type WorkflowInterceptorsFactory } from '@temporalio/workflow';

/**
 * A caller's workflow interceptor, which runs before the plugin's: for `lastFailureOfRetryAfterActivity`
 * it awaits a failing activity and catches its failure before it starts the workflow.
 */
// ts-prune-ignore-next
export const interceptors: WorkflowInterceptorsFactory = () => ({
  inbound: [
    {
      async execute(input, next) {
        if (workflowInfo().workflowType === 'lastFailureOfRetryAfterActivity') {
          const { failingActivity } = proxyActivities<{ failingActivity(message: string): Promise<void> }>({
            startToCloseTimeout: '10 seconds',
            retry: { maximumAttempts: 1 },
          });
          await failingActivity('before the workflow').catch(() => undefined);
        }
        return await next(input);
      },
    },
  ],
});
