"""Sends Claude Code's stdout to its Workflow: a Signal for each line."""

from __future__ import annotations

from typing import Any, cast

from claude_agent_sdk import CLIJSONDecodeError
from claude_agent_sdk._internal.transport.subprocess_cli import _parse_stdout_line

from temporalio.client import WorkflowHandle
from temporalio.contrib.workflow_streams import WorkflowStreamClient

from .._signals import CliOutput, OutputKind


class OutputSender:
    """Sends each line of Claude Code's stdout to its Workflow as a :class:`CliOutput` Signal.

    With ``stream``, ``stream_event`` lines are published to ``topic`` instead.
    The Workflow has all the output before byte :attr:`start`, and a retried
    Activity resumes from there. :attr:`start` passes a published line only
    after the stream flushed it. So a retry can publish a line twice, but never
    loses one.
    """

    def __init__(
        self,
        workflow: WorkflowHandle[Any, Any],
        session: str,
        offset: int,
        stream: WorkflowStreamClient | None = None,
        topic: str = "",
    ) -> None:
        """Send the output of ``session``'s Claude Code from byte ``offset``."""
        self._workflow = workflow
        self._session = session
        self._stream = stream
        self._topic = stream.topic(topic) if stream is not None else None
        # The Workflow has every line before ``start``. The lines from there to
        # ``end`` were published to the stream, but maybe not flushed yet.
        self.start = self.end = offset
        self._unflushed = False

    async def send(self, line: bytes) -> None:
        """Send ``line``, the next line of stdout, with its newline if it has one."""
        end = self.end + len(line)
        text = line.removesuffix(b"\n").decode(errors="replace")
        try:
            message = _parse_stdout_line(text)
        except CLIJSONDecodeError:
            if not line.endswith(b"\n"):
                # As in the Claude Agent SDK, a last line cut off mid-write is dropped.
                return
            # Signal it anyway. As in the Claude Agent SDK, the transport fails on it.
            message = None
        if (
            self._topic is not None
            and message
            and message.get("type") == "stream_event"
        ):
            self._topic.publish(message)
            self._unflushed = True
            self.end = end
            return
        if self._stream is not None and self._unflushed:
            # Flush first, as start is about to move past the published lines.
            await self._stream.flush()
            self._unflushed = False
        kind, label = _classify(message)
        output = CliOutput(self.start, end, text, session=self._session, summary=label)
        await self._workflow.signal(kind, output)
        self.start = self.end = end


def _classify(message: dict[str, Any] | None) -> tuple[OutputKind, str]:
    """The kind of a stdout message, which names its Signal, and a label for the Temporal UI."""
    if message is None:
        return "other", "not a message"
    kind = str(message.get("type"))
    if kind in ("control_request", "control_response"):
        body = message.get("request") or message.get("response")
        body = body if isinstance(body, dict) else {}
        # can_use_tool names its tool, and mcp_message its server.
        about = body.get("tool_name") or body.get("server_name")
        subtype = body.get("subtype")
        label = f"{kind}: {subtype} {about}" if about else f"{kind}: {subtype}"
        return cast(OutputKind, kind), label
    if kind == "control_cancel_request":
        return "control_request", kind
    if kind == "result":
        return "result", f"result: {message.get('subtype')}"
    if kind == "system":
        return "other", f"system: {message.get('subtype')}"
    if kind not in ("assistant", "user"):
        return "other", kind
    body = message.get("message")
    content = body.get("content") if isinstance(body, dict) else None
    blocks: list[dict[str, Any]] = (
        [b for b in content if isinstance(b, dict)] if isinstance(content, list) else []
    )
    tools = [str(b.get("name")) for b in blocks if b.get("type") == "tool_use"]
    if tools:
        return "tool_use", f"tool_use: {', '.join(tools)}"
    if any(b.get("type") == "tool_result" for b in blocks):
        return "tool_result", "tool_result"
    if kind == "user":
        return "other", kind
    return "assistant", f"assistant: {', '.join(str(b.get('type')) for b in blocks)}"
