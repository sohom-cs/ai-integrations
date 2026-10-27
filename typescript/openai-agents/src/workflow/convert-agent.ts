import { type Agent, Handoff } from '@openai/agents-core';
import { ApplicationFailure } from '@temporalio/common';
import { patched } from '@temporalio/workflow';
import type { ModelActivityOptions } from '../common/model-activity-options';
import { ActivityBackedModel } from './activity-backed-model';

/**
 * Runs that handed off to a custom `Handoff`'s `agent` field, when its callback
 * returned another agent, keep doing so on replay: that agent's model call and
 * tools would not match their history.
 */
const HANDOFF_RETURNED_AGENT_PATCH = 'openai-agents.handoff-returned-agent';

/**
 * Recursively converts an agent graph: validate tools, replace each agent's
 * model with an ActivityBackedModel, and clone handoffs. Uses a seen-map to
 * handle circular handoff references.
 */
export function convertAgent(
  agent: Agent<any, any>,
  modelParams: ModelActivityOptions,
  seen?: Map<Agent<any, any>, Agent<any, any>>,
  modelNameOverride?: string
): Agent<any, any> {
  seen = seen ?? new Map();
  if (seen.has(agent)) return seen.get(agent)!;

  // Reject raw functions: authoring mistake — use tool() or activityAsTool().
  const tools = agent.tools ?? [];
  for (const t of tools) {
    if (typeof t === 'function') {
      throw ApplicationFailure.create({
        message:
          `Agent '${agent.name}': Provided tool is a raw function, not a tool object. ` +
          'Did you mean to use tool() or activityAsTool()?',
        type: 'AgentsWorkflowError',
        nonRetryable: true,
      });
    }
  }

  const rawModel = agent.model;
  if (rawModel !== undefined && rawModel !== null && typeof rawModel !== 'string') {
    throw ApplicationFailure.create({
      message:
        `Agent '${agent.name}' has a Model object instead of a string model name. ` +
        'In Temporal workflows, all models must be specified as strings — use ' +
        'runConfig.model to override, or declare a string model on the agent.',
      type: 'AgentsWorkflowError',
      nonRetryable: true,
    });
  }
  const modelName = modelNameOverride ?? (typeof rawModel === 'string' ? rawModel : undefined);
  if (modelName === undefined) {
    throw ApplicationFailure.create({
      message:
        `Agent '${agent.name}' has no model declared and no runConfig.model override given. ` +
        'Declare a model on the agent or pass runConfig.model to runner.run().',
      type: 'AgentsWorkflowError',
      nonRetryable: true,
    });
  }
  const activityBackedModel = new ActivityBackedModel(modelName, modelParams);
  // Bind to the original (pre-clone) agent so the summary provider sees the
  // user-declared name and instructions.
  activityBackedModel.setAgent(agent);

  const converted = agent.clone({ model: activityBackedModel });
  seen.set(agent, converted);

  const convertedHandoffs = (agent.handoffs ?? []).map((h) => {
    if (h instanceof Handoff) {
      const convertedHandoffAgent = convertAgent(h.agent, modelParams, seen, modelNameOverride);
      // The run hands off to the agent the callback returns, which a custom
      // Handoff can choose at invocation time, so convert that agent.
      return h.clone({
        agent: convertedHandoffAgent,
        onInvokeHandoff: async (context, args) => {
          const target = await h.onInvokeHandoff(context, args);
          if (target === h.agent || !patched(HANDOFF_RETURNED_AGENT_PATCH)) return convertedHandoffAgent;
          return convertAgent(target, modelParams, seen, modelNameOverride);
        },
      });
    }
    return convertAgent(h, modelParams, seen, modelNameOverride);
  });

  converted.handoffs = convertedHandoffs;
  return converted;
}
