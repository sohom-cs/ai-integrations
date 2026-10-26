"""Runs the Worker for AgentWorkflow: ``python -m samples.worker``.

Start a dev server:

    temporal server start-dev

Claude Code runs on this Worker, with its environment. Give it credentials by
setting ``ANTHROPIC_API_KEY`` or ``CLAUDE_CODE_OAUTH_TOKEN`` for the Worker, so
they are never in history.
"""

import asyncio

from samples.workflow import AgentWorkflow, roll_dice
from temporalio.claude_agent_sdk import (
    ClaudeAgentSDKPlugin,
)
from temporalio.client import Client
from temporalio.worker import Worker

TASK_QUEUE = "claude-agent-sdk"


def create_worker(client: Client) -> Worker:
    return Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[AgentWorkflow],
        activities=[roll_dice],
        plugins=[ClaudeAgentSDKPlugin()],
    )


async def main() -> None:
    client = await Client.connect("localhost:7233")
    await create_worker(client).run()


if __name__ == "__main__":
    asyncio.run(main())
