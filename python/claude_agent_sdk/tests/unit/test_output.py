"""How far into the CLI's stdout the watch Activity has sent, which its retry resumes from."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

import pytest

from temporalio.claude_agent_sdk._activity._output import OutputSender
from temporalio.claude_agent_sdk._signals import CliOutput


def assistant(*content: dict[str, Any]) -> dict[str, Any]:
    return {"type": "assistant", "message": {"content": list(content)}}


def ndjson(*messages: dict[str, Any]) -> bytes:
    return "".join(json.dumps(message) + "\n" for message in messages).encode()


async def send(output: OutputSender, *messages: dict[str, Any]) -> None:
    for message in messages:
        await output.send(ndjson(message))


class FakeWorkflow:
    """Records each Signal the sender sends, and what was recorded before it."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.signals: list[tuple[str, CliOutput]] = []
        self.events = events if events is not None else []

    async def signal(self, signal: str, output: CliOutput) -> None:
        self.events.append(f"signal {signal}")
        self.signals.append((signal, output))


class FakeStream:
    """A stream client that records its publishes and flushes in ``events``."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.flush_fails = False

    def topic(self, name: str) -> Any:
        def publish(message: dict[str, Any]) -> None:
            self.events.append("publish")
            self.published.append((name, message))

        return SimpleNamespace(publish=publish)

    async def flush(self) -> None:
        self.events.append("flush")
        if self.flush_fails:
            raise TimeoutError("The stream is gone")


async def test_each_line_is_signalled_with_its_byte_offsets_from_the_offset_given() -> (
    None
):
    workflow = FakeWorkflow()
    first, second = ndjson({"type": "a"}), ndjson({"type": "b"})
    output = OutputSender(cast(Any, workflow), "s", 100)

    await output.send(first)
    await output.send(second)
    # The last line of a CLI that stopped may have no newline.
    await output.send(b'{"type": "c"}')

    [(_, a), (_, b), (_, c)] = workflow.signals
    assert (a.start, a.end, a.line) == (100, 100 + len(first), '{"type": "a"}')
    assert (b.start, b.end) == (100 + len(first), 100 + len(first + second))
    assert (c.start, c.end, c.line) == (b.end, b.end + 13, '{"type": "c"}')
    assert (output.start, output.end) == (c.end,) * 2


async def test_a_last_line_cut_off_mid_write_is_dropped() -> None:
    workflow = FakeWorkflow()
    output = OutputSender(cast(Any, workflow), "s", 0)

    await output.send(b'{"type": "res')

    assert workflow.signals == []


async def test_a_published_line_is_flushed_before_the_next_signal() -> None:
    events: list[str] = []
    workflow, stream = FakeWorkflow(events), FakeStream(events)
    delta = {"type": "stream_event", "event": {"type": "content_block_delta"}}
    first, answer, last = (
        assistant({"type": "thinking"}),
        assistant({"type": "text"}),
        {"type": "result"},
    )
    output = OutputSender(cast(Any, workflow), "s", 0, cast(Any, stream), "partial")

    await send(output, first, delta, delta)

    # The offset stays before the published lines, which may not be flushed yet.
    assert events == ["signal assistant", "publish", "publish"]
    assert output.start == len(ndjson(first))
    assert output.end == len(ndjson(first, delta, delta))

    await send(output, answer, last)

    assert events == [
        "signal assistant",
        "publish",
        "publish",
        "flush",
        "signal assistant",
        "signal result",
    ]
    assert stream.published == [("partial", delta)] * 2
    # The Signal after the published lines starts before them: the Workflow skips them.
    assert workflow.signals[1][1].start == len(ndjson(first))
    assert output.start == output.end == len(ndjson(first, delta, delta, answer, last))


async def test_the_offset_does_not_pass_a_published_line_that_failed_to_flush() -> None:
    events: list[str] = []
    workflow, stream = FakeWorkflow(events), FakeStream(events)
    stream.flush_fails = True
    delta = {"type": "stream_event"}
    output = OutputSender(cast(Any, workflow), "s", 0, cast(Any, stream), "partial")

    with pytest.raises(TimeoutError):
        await send(output, delta, {"type": "result"})

    assert workflow.signals == []
    assert output.start == 0
