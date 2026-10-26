"""The Activity that merges ``options.sandbox`` into ``options.settings`` for a transport."""

from __future__ import annotations

import asyncio

from claude_agent_sdk import ClaudeAgentOptions, SandboxSettings
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

from temporalio import activity

MERGE_SETTINGS = "claude-code-merge-settings"


@activity.defn(name=MERGE_SETTINGS)
async def merge_settings(settings: str, sandbox: SandboxSettings) -> str:
    """Merge ``sandbox`` into ``settings`` with the Claude Agent SDK's own code, and return the result as JSON.

    ``settings`` can be a file path, which the Claude Agent SDK reads. Doing
    that here keeps file I/O out of the Workflow.
    """
    options = ClaudeAgentOptions(settings=settings, sandbox=sandbox)
    sdk = SubprocessCLITransport(prompt="", options=options)
    return await asyncio.to_thread(sdk._build_settings_value) or "{}"
