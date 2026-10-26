"""Worker plugin that lets Workflows use the Claude Agent SDK and runs a transport's Activities."""

from __future__ import annotations

import dataclasses
import importlib
from collections.abc import Callable, Sequence
from typing import Any

from claude_agent_sdk import ClaudeSDKError

from temporalio.plugin import SimplePlugin
from temporalio.worker import WorkflowRunner
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from ._activity._settings import merge_settings
from ._subprocess._activities import subprocess_activities


class ClaudeAgentSDKPlugin(SimplePlugin):
    """Run ``ClaudeSDKClient`` and ``query`` in Workflows.

    Give them a :class:`TemporalTransport` as ``transport``, and give this
    plugin the Activities that transport runs.
    """

    def __init__(
        self, *, activities: Sequence[Callable[..., Any]] | None = None
    ) -> None:
        """Create the plugin.

        Args:
            activities: The Activities your transports run. The plugin
                registers them, plus the one every transport uses to merge
                settings. Defaults to :func:`subprocess_activities`.
        """
        if activities is None:
            activities = subprocess_activities()

        def workflow_runner(runner: WorkflowRunner | None) -> WorkflowRunner:
            if not runner:
                raise ValueError("No WorkflowRunner provided to ClaudeAgentSDKPlugin.")
            if isinstance(runner, SandboxedWorkflowRunner):
                # Import anyio's asyncio backend here, outside the sandbox.
                # Inside, anyio imports it lazily with import_module, which
                # skips the passthrough below and fails on its threading imports.
                importlib.import_module("anyio._backends._asyncio")
                return dataclasses.replace(
                    runner,
                    # Pass these through, so the Workflow shares one copy of
                    # them with the Worker. anyio, the Claude Agent SDK's async
                    # runtime, uses threads, and sniffio keeps the running
                    # async library in a thread-local.
                    restrictions=runner.restrictions.with_passthrough_modules(
                        "claude_agent_sdk", "anyio", "sniffio"
                    ),
                )
            return runner

        super().__init__(
            name="ClaudeAgentSDKPlugin",
            workflow_runner=workflow_runner,
            # Fail the Workflow on a Claude Agent SDK error such as ProcessError,
            # as it would end a program. Otherwise Temporal retries the Workflow
            # Task forever.
            workflow_failure_exception_types=[ClaudeSDKError],
            activities=[*activities, merge_settings],
        )
