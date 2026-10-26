"""The body of a transport's watch Activity: Claude Code's stdout to the Workflow, until it exits."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence

from temporalio import activity
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.exceptions import ApplicationError

from .._errors import process_error
from .._signals import STDERR, CliOutput
from ._heartbeat import heartbeating
from ._output import OutputSender


async def watch_output(
    session_id: str,
    stdout: Callable[[int], AsyncIterator[bytes]],
    exit_status: Callable[[], Awaitable[int | None]],
    stderr: Callable[[int], AsyncIterator[bytes]],
    *,
    stream_topic: str | None = None,
    signal_stderr: bool = False,
) -> None:
    """Send Claude Code's stdout, and stderr if asked, to the Workflow until it exits. Call it in your watch Activity.

    Each line goes to the transport as a :class:`CliOutput` Signal. With
    ``stream_topic``, ``stream_event`` lines go to that topic instead. It
    heartbeats how far it got, and a retry of the Activity resumes there.

    Args:
        session_id: The transport's ``session_id``.
        stdout: Called with a byte offset. Yields Claude Code's stdout from
            there in lines, each with its newline, until Claude Code no
            longer runs. The last line may have no newline.
        exit_status: Called once ``stdout`` ends. Returns Claude Code's exit
            status, or ``None`` if it was lost without one, such as when its
            container was killed.
        stderr: Like ``stdout``, for Claude Code's stderr.
        stream_topic: The name of the transport's ``stream``, if it has one.
        signal_stderr: Whether to Signal Claude Code's stderr too: the
            ``WatchRequest``'s ``signal_stderr``.

    Raises:
        ApplicationError: If Claude Code exited with a nonzero status. This
            fails the Activity for good, and the transport raises
            ``ProcessError``. Also raised if no Workflow started the Activity.
        RuntimeError: If Claude Code was lost. The Activity is retried under
            its retry policy.
    """
    info = activity.info()
    if info.workflow_id is None:
        raise ApplicationError(
            "watch_output needs an Activity that a Workflow started",
            non_retryable=True,
        )
    details: Sequence[list[int]] = info.heartbeat_details
    # How far the Workflow has stdout and stderr.
    stdout_offset, stderr_offset = details[0] if details else (0, 0)
    workflow = activity.client().get_workflow_handle(
        info.workflow_id, run_id=info.workflow_run_id
    )
    stream = None
    if stream_topic is not None:
        stream = WorkflowStreamClient.from_within_activity()
    output = OutputSender(
        workflow, session_id, stdout_offset, stream, stream_topic or ""
    )

    async def send_stderr() -> None:
        nonlocal stderr_offset
        async for line in stderr(stderr_offset):
            end = stderr_offset + len(line)
            text = line.removesuffix(b"\n").decode(errors="replace")
            error = CliOutput(stderr_offset, end, text, session=session_id)
            await workflow.signal(STDERR, error)
            stderr_offset = end

    # Heartbeat through the stream's last flush and the exit status too, as
    # both can be slow.
    async with heartbeating(lambda: [output.start, stderr_offset]):
        sending_stderr = asyncio.ensure_future(send_stderr()) if signal_stderr else None
        try:
            async with stream if stream is not None else contextlib.nullcontext():
                async for line in stdout(output.end):
                    await output.send(line)
            if sending_stderr is not None:
                await sending_stderr
        finally:
            if sending_stderr is not None:
                sending_stderr.cancel()
        status = await exit_status()
        if status is None:
            raise RuntimeError("Claude Code's output ended without a recorded exit")
        if status != 0:
            raise process_error(status, b"".join([line async for line in stderr(0)]))
