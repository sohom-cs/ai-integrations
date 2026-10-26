"""TemporalTransport's handling of the CLI's stdout, with the test sending it as watch_output does."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from claude_agent_sdk import ClaudeAgentOptions

from temporalio import workflow
from temporalio.claude_agent_sdk import (
    TemporalTransport,
)
from temporalio.claude_agent_sdk._command import ClaudeCodeCommand
from temporalio.claude_agent_sdk._signals import CliOutput
from temporalio.client import (
    WorkflowFailureError,
    WorkflowHandle,
    WorkflowQueryFailedError,
)
from temporalio.contrib.workflow_streams import WorkflowTopicHandle
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from tests.helpers.workflows import start_workflow

# Lines of the CLI's stdout. The transport hands them on as they are.
STEPS = [{"type": "assistant", "n": n} for n in range(3)]
RESULT = {"type": "result"}


@workflow.defn
class TransportWorkflow:
    """Writes ``lines``, and reads what reaches the Claude Agent SDK up to a result.

    The transport runs no CLI: the test sends the CLI's stdout itself.
    """

    def __init__(self) -> None:
        self.transport: TemporalTransport | None = None

    @workflow.run
    async def run(
        self, lines: list[dict[str, Any]], max_buffer_size: int | None = None
    ) -> list[Any]:
        options = ClaudeAgentOptions(max_buffer_size=max_buffer_size)
        transport = TemporalTransport(options)
        transport._connected = True
        self.transport = transport
        await transport.write("".join(json.dumps(line) + "\n" for line in lines))
        messages: list[Any] = []
        async for message in transport.read_messages():
            messages.append(message)
            if message["type"] == "result":
                break
        return messages

    @workflow.query
    def session(self) -> str:
        assert self.transport is not None
        return self.transport.session_id

    @workflow.query
    def outbox(self) -> list[dict[str, Any]]:
        """Lines waiting for the CLI's stdin."""
        assert self.transport is not None
        return self.transport._outbox


class Cli:
    """Sends lines of stdout to the Workflow, each in a Signal of its own, as watch_output does."""

    def __init__(self, handle: WorkflowHandle[Any, Any], session: str) -> None:
        self.handle = handle
        self.session = session
        self.lines: list[str] = []

    def add(self, *messages: dict[str, Any]) -> None:
        self.lines += [json.dumps(message) for message in messages]

    async def send(self, first: int, last: int, session: str | None = None) -> None:
        """Send lines ``first`` up to ``last``, as the CLI of ``session``, or of the transport."""
        offsets = [0]
        for line in self.lines:
            offsets.append(offsets[-1] + len(line.encode()) + 1)
        for at in range(first, last):
            output = CliOutput(
                offsets[at],
                offsets[at + 1],
                self.lines[at],
                session=session or self.session,
            )
            await self.handle.signal("other", output)


async def query_once_running(handle: WorkflowHandle[Any, Any], query: Any) -> Any:
    """The query's answer, once the Workflow has made what it asks about."""
    while True:
        with contextlib.suppress(WorkflowQueryFailedError):
            return await handle.query(query)
        await asyncio.sleep(0.05)


@contextlib.asynccontextmanager
async def transport_workflow(
    env: WorkflowEnvironment, lines: list[Any], max_buffer_size: int | None = None
) -> AsyncIterator[Cli]:
    """Start a TransportWorkflow that writes ``lines``, and its CLI once it has a transport."""
    async with start_workflow(
        env.client, TransportWorkflow, lines, max_buffer_size
    ) as handle:
        session = await query_once_running(handle, TransportWorkflow.session)
        yield Cli(handle, session)


def test_the_cli_gets_the_options_env() -> None:
    options = ClaudeAgentOptions(
        enable_file_checkpointing=True, env={"MY_SETTING": "kept"}
    )

    env = ClaudeCodeCommand(options).env()

    assert env["MY_SETTING"] == "kept"
    # As the Claude Agent SDK's own transport sets it for enable_file_checkpointing.
    assert env["CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING"] == "true"


async def test_each_line_reaches_the_sdk_once_in_order(
    env: WorkflowEnvironment,
) -> None:
    """A line comes twice when a watch_output is retried, ahead of the lines before it when a stale attempt sends it, or late from an earlier transport's CLI. Only the next one of its own CLI is taken: the retry sends the rest in order."""
    async with transport_workflow(env, []) as cli:
        cli.add(*STEPS, RESULT)
        await cli.send(0, 1, session="earlier")  # Another CLI's: ignored.
        await cli.send(2, 4)  # Ahead of the offset: ignored.
        await cli.send(0, 2)
        await cli.send(0, 2)  # Again: ignored.
        await cli.send(2, 4)  # Sent again, in order.
        messages = await cli.handle.result()

    assert messages == [*STEPS, RESULT]


async def test_the_sdks_requests_go_under_ids_the_workflow_picks(
    env: WorkflowEnvironment,
) -> None:
    """The Claude Agent SDK's IDs are random, so a replayed Workflow would send different ones."""
    interrupt = {
        "type": "control_request",
        "request_id": "sdk_random",
        "request": {"subtype": "interrupt"},
    }
    async with transport_workflow(env, [interrupt]) as cli:
        [sent] = await cli.handle.query(TransportWorkflow.outbox)
        response = {"subtype": "success", "request_id": sent["request_id"]}
        cli.add({"type": "control_response", "response": response}, RESULT)
        await cli.send(0, 2)
        messages = await cli.handle.result()

    assert sent["request_id"] != "sdk_random"
    assert messages[0]["response"]["request_id"] == "sdk_random"


@pytest.mark.parametrize("partial", [True, False])
def test_a_stream_goes_with_partial_messages_and_only_them(partial: bool) -> None:
    topic = WorkflowTopicHandle[Any](cast(Any, None), "partial", cast(Any, Any))
    options = ClaudeAgentOptions(include_partial_messages=partial)
    with pytest.raises(ValueError, match="stream"):
        TemporalTransport(options, stream=None if partial else topic)


async def test_a_line_that_is_not_a_json_object_is_skipped(
    env: WorkflowEnvironment,
) -> None:
    """As the Claude Agent SDK skips it, such as ``[SandboxDebug] ...``."""
    async with transport_workflow(env, []) as cli:
        cli.lines.append("[SandboxDebug] x")
        cli.add(RESULT)
        await cli.send(0, 2)
        messages = await cli.handle.result()

    assert messages == [RESULT]


async def test_a_line_over_max_buffer_size_fails_as_in_the_sdk(
    env: WorkflowEnvironment,
) -> None:
    async with transport_workflow(env, [], max_buffer_size=10) as cli:
        cli.add(RESULT, RESULT)
        await cli.send(0, 1)
        with pytest.raises(WorkflowFailureError) as raised:
            await cli.handle.result()

    error = raised.value.cause
    assert isinstance(error, ApplicationError)
    assert error.type == "CLIJSONDecodeError"
    assert "maximum buffer size of 10 bytes" in error.message
