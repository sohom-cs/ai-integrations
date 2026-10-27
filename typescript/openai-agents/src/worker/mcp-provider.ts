import type { MCPServer } from '@openai/agents-core';
import { log } from '@temporalio/activity';
import { MCP_CALL_TOOL_SUFFIX, MCP_LIST_TOOLS_SUFFIX, MCP_STATELESS_SUFFIX } from '../common/mcp-types';

/**
 * Applies the Worker-side server's static `toolFilter`, as the agent does for a native server.
 * A callable filter needs the agent and the run context, which only the Workflow has, so it is
 * skipped with a warning.
 */
export function filterListedTools<T extends { name: string }>(server: MCPServer, tools: T[]): T[] {
  const filter = server.toolFilter;
  if (!filter) {
    return tools;
  }
  if (typeof filter === 'function') {
    log.warn(
      `MCP server '${server.name}' has a callable toolFilter, which the Worker cannot run. ` +
        'Pass it as the toolFilter option of statelessMcpServer() or statefulMcpServer() in the Workflow.'
    );
    return tools;
  }
  const allowed = filter.allowedToolNames ?? [];
  const blocked = filter.blockedToolNames ?? [];
  return tools.filter((tool) => (allowed.length === 0 || allowed.includes(tool.name)) && !blocked.includes(tool.name));
}

export class StatelessMCPServerProvider {
  private readonly _name: string;

  constructor(
    public readonly name: string,
    private serverFactory: (factoryArgument?: unknown) => MCPServer
  ) {
    this._name = `${name}${MCP_STATELESS_SUFFIX}`;
  }

  _getActivities(): Record<string, (...args: any[]) => Promise<unknown>> {
    return {
      [`${this._name}${MCP_LIST_TOOLS_SUFFIX}`]: async (input?: { factoryArgument?: unknown }) => {
        const s = this.serverFactory(input?.factoryArgument);
        try {
          await s.connect();
          return filterListedTools(s, await s.listTools());
        } finally {
          await s.close();
        }
      },
      [`${this._name}${MCP_CALL_TOOL_SUFFIX}`]: async (input: {
        toolName: string;
        args: Record<string, unknown> | null;
        factoryArgument?: unknown;
      }) => {
        const s = this.serverFactory(input.factoryArgument);
        try {
          await s.connect();
          return await s.callTool(input.toolName, input.args);
        } finally {
          await s.close();
        }
      },
    };
  }
}
