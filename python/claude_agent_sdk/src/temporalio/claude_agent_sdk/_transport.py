"""A Claude Agent SDK ``Transport`` for Workflows, which reaches Claude Code through Activities."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, NamedTuple, TypeVar

from claude_agent_sdk import (
    ClaudeAgentOptions,
    CLIConnectionError,
    CLIJSONDecodeError,
    Transport,
)
from claude_agent_sdk._internal.transport.subprocess_cli import (
    _DEFAULT_MAX_BUFFER_SIZE,
    _parse_stdout_line,
)

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.contrib.workflow_streams import WorkflowTopicHandle
from temporalio.exceptions import ActivityError, is_cancelled_exception

from ._activity._settings import merge_settings
from ._cli_files import CliFiles
from ._command import ClaudeCodeCommand
from ._errors import sdk_error
from ._signals import OUTPUT_KINDS, STDERR, CliOutput

# How the Temporal UI names the stdin lines an Activity sends.
_STDIN_LABELS = {
    "user": "prompt",
    "control_request": "request",
    "control_response": "reply",
}
# Retry forever, because Claude Code can run for days. Wait at most 5 seconds
# between retries, because the Claude Agent SDK is waiting for the output.
_WATCH_RETRY_POLICY = RetryPolicy(maximum_interval=timedelta(seconds=5))

_Arg = TypeVar("_Arg")
_Result = TypeVar("_Result")


@dataclass
class LaunchRequest:
    """The launch Activity's input: start Claude Code with ``args`` and ``env``.

    They're built from the options as in the Claude Agent SDK.
    """

    session_id: str
    args: list[str]
    env: dict[str, str]
    cli_path: str | None = None
    """``options.cli_path``, or ``None`` to find Claude Code as in the Claude Agent SDK."""
    cwd: str | None = None
    """``options.cwd``."""
    user: str | None = None
    """``options.user``, the user to run Claude Code as. A transport that can't do that should fail the launch."""


@dataclass
class StdinWrite:
    """The write Activity's input: write ``text``, one or more lines, to Claude Code's stdin.

    Writes are numbered from 1, so a retry can write the same one again
    instead of adding a second. The last write is :attr:`CliFiles.END_OF_INPUT`,
    which closes stdin. Claude Code then exits once it's done, as in the Claude
    Agent SDK.
    """

    session_id: str
    handle: str
    """What the launch Activity returned."""
    number: int
    text: str


@dataclass
class WatchRequest:
    """The watch Activity's input. Pass these to :func:`~temporalio.claude_agent_sdk.watch_output`."""

    session_id: str
    handle: str
    """What the launch Activity returned."""
    stream_topic: str | None = None
    signal_stderr: bool = False


@dataclass
class StopRequest:
    """The stop Activity's input: stop Claude Code and everything it started."""

    session_id: str
    handle: str | None
    """What the launch Activity returned, or ``None`` if it was cancelled before it returned."""


class TransportActivities(NamedTuple):
    """The Activities a :class:`TemporalTransport` runs. Register them with ``ClaudeAgentSDKPlugin``."""

    launch: Callable[[LaunchRequest], Awaitable[str]]
    """Start Claude Code, unless it started already. Return a handle, such as a host or container ID, that the other Activities get."""
    write_stdin: Callable[[StdinWrite], Awaitable[None]]
    watch_output: Callable[[WatchRequest], Awaitable[None]]
    stop: Callable[[StopRequest], Awaitable[None]]


class TemporalTransport(Transport):
    """A Claude Agent SDK ``Transport`` for Workflows.

    Pass it to ``ClaudeSDKClient`` or ``query`` as ``transport``. It runs
    ``activities`` to start Claude Code, carry its input and output, and stop
    it, and does everything else in the Workflow.
    """

    def __init__(
        self,
        options: ClaudeAgentOptions,
        activities: TransportActivities | None = None,
        *,
        task_queue: str | None = None,
        start_to_close_timeout: timedelta = timedelta(minutes=5),
        heartbeat_timeout: timedelta = timedelta(seconds=10),
        retry_policy: RetryPolicy | None = None,
        stream: WorkflowTopicHandle[Any] | None = None,
    ) -> None:
        """Prepare the transport. Claude Code starts on ``connect``.

        Args:
            options: The same options passed to ``ClaudeSDKClient``. Claude
                Code's command line is built from them.
            activities: The Activities that run Claude Code. Defaults to
                :func:`subprocess_activities`.
            task_queue: The task queue for the Activities. Defaults to the
                Workflow's.
            start_to_close_timeout: The timeout of each attempt of every
                Activity except the watch, which runs as long as Claude Code.
            heartbeat_timeout: How soon a lost Worker is detected, for every
                Activity.
            retry_policy: The retry policy of every Activity. ``None`` uses
                Temporal's default, which retries forever. The watch then waits
                at most 5 seconds between retries, because the Claude Agent SDK
                is waiting for its output.
            stream: A topic of the Workflow's ``WorkflowStream``. Required
                when ``options.include_partial_messages`` is set, and not
                allowed otherwise. The watch Activity publishes each
                ``stream_event`` line to it instead of Signalling it. Only
                observers outside the Workflow read it.

        Raises:
            ValueError: If ``options.include_partial_messages`` is set without
                ``stream``, or ``stream`` without it.
        """
        if options.include_partial_messages != (stream is not None):
            name = type(self).__name__
            raise ValueError(
                f"{name} needs a stream if and only if "
                "options.include_partial_messages is set"
            )
        self._options = options
        if activities is None:
            # Imported here to avoid a circular import.
            from ._subprocess._activities import subprocess_activities

            activities = subprocess_activities()
        self._activities = activities
        self._task_queue = task_queue
        self._start_to_close_timeout = start_to_close_timeout
        self._heartbeat_timeout = heartbeat_timeout
        self._retry_policy = retry_policy
        self._stream_topic = stream.name if stream is not None else None
        self.session_id = str(workflow.uuid4())
        """The deterministic ID of this transport's Claude Code."""
        # What the launch Activity returned.
        self._handle: str | None = None

        # The CLI's output comes on these Signals. A later transport in the same
        # Workflow takes them over, so run one transport at a time.
        for kind in OUTPUT_KINDS:
            workflow.set_signal_handler(kind, self._on_output)
        workflow.set_signal_handler(STDERR, self._on_stderr)
        # How far into the CLI's stderr the Workflow has the lines.
        self._stderr_offset = 0

        # Messages for the CLI's stdin, and for the Claude Agent SDK.
        self._outbox: list[dict[str, Any]] = []
        self._inbox: deque[dict[str, Any]] = deque()
        # How far into the CLI's stdout the Workflow has the lines.
        self._stdout_offset = 0
        # The Claude Agent SDK picks random request IDs, which replay wouldn't
        # reproduce. Send IDs from workflow.uuid4 instead, and map each back to
        # the Claude Agent SDK's when the CLI answers.
        self._sdk_ids: dict[str, str] = {}
        self._tasks: list[asyncio.Task[None]] = []
        # The launch task. Once it exists, close cancels it and stops the CLI.
        self._launching: asyncio.Task[None] | None = None
        self._connected = False
        self._closed = False
        # The Claude Agent SDK called end_input.
        self._ended = False
        # Watching the output ended, so the CLI is gone. Without a failure, it
        # exited with status 0, and read_messages ends.
        self._exited = False
        self._failure: BaseException | None = None

    async def connect(self) -> None:
        """Start Claude Code.

        As in the Claude Agent SDK, ``options.sandbox`` is merged into
        ``options.settings``. An Activity on the Workflow's task queue does
        the merge, since ``options.settings`` can be a file path.

        Raises:
            CLINotFoundError: If there is no Claude Code to run.
            CLIConnectionError: If Claude Code can't start, such as when its
                working directory doesn't exist.
        """
        self._connected = True
        try:
            await self._merge_settings()
            self._launching = asyncio.create_task(self._launch())
            await self._launching
        except BaseException as error:
            # The Claude Agent SDK doesn't close a transport that failed to
            # connect, so close it here. Raise the launch error even if closing
            # fails too.
            try:
                await self.close()
            finally:
                raise _raised(error)  # noqa: B904 - sdk_error sets the cause.
        self._tasks.append(asyncio.create_task(self._receive_output()))
        self._tasks.append(asyncio.create_task(self._send_input()))

    async def write(self, data: str) -> None:
        """Send the Claude Agent SDK's prompts, requests, and answers to the CLI.

        Raises:
            CLIConnectionError: If the transport isn't connected, or the CLI exited.
        """
        if not self.is_ready():
            raise CLIConnectionError("ProcessTransport is not ready for writing")
        if self._failure is not None:
            raise CLIConnectionError(
                f"Cannot write to process that exited with error: {self._failure}"
            ) from self._failure
        if self._exited:
            raise CLIConnectionError("Cannot write to terminated process")
        for line in data.splitlines():
            if not line.strip():
                continue
            message: dict[str, Any] = json.loads(line)
            if message["type"] == "control_request":
                request_id = str(workflow.uuid4())
                self._sdk_ids[request_id] = message["request_id"]
                message = {**message, "request_id": request_id}
            self._outbox.append(message)

    async def read_messages(self) -> AsyncIterator[dict[str, Any]]:
        """Yield the CLI's messages, each once, until the transport closes or the CLI exits.

        Raises:
            CLIConnectionError: If the transport isn't connected.
            ProcessError: If the CLI exited with a nonzero status.
            CLIJSONDecodeError: If the CLI wrote a line that looks like JSON but isn't.
        """
        if not self._connected:
            raise CLIConnectionError("Not connected")
        while True:
            await workflow.wait_condition(
                lambda: (
                    bool(self._inbox)
                    or self._failure is not None
                    or self._closed
                    or self._exited
                )
            )
            if self._inbox:
                yield self._inbox.popleft()
            elif self._failure is not None:
                raise self._failure
            else:
                await self.close()
                return

    async def end_input(self) -> None:
        """End the CLI's stdin. As in the Claude Agent SDK, the CLI exits once it is done."""
        self._ended = True

    async def close(self) -> None:
        """Stop the CLI and everything it started."""
        if self._closed:
            return
        self._closed = True
        for task in self._tasks:
            task.cancel()
        if self._launching is not None:
            # Launching can retry forever: cancel it, then stop the CLI.
            self._launching.cancel()
            await asyncio.wait([self._launching])
            stop = StopRequest(self.session_id, self._handle)
            await self._execute(self._activities.stop, stop, "Stop Claude Code")

    async def _launch(self) -> None:
        """Run the launch Activity. Cancelling it waits for the Activity to stop, so close then stops the CLI it started."""
        options = self._options
        command = ClaudeCodeCommand(options)
        request = LaunchRequest(
            self.session_id,
            command.args(),
            command.env(),
            cli_path=str(options.cli_path) if options.cli_path else None,
            cwd=str(options.cwd) if options.cwd else None,
            user=options.user,
        )
        self._handle = await self._execute(
            self._activities.launch,
            request,
            "Start Claude Code",
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )

    async def _write_stdin(self, number: int, text: str, summary: str) -> None:
        """Run the write Activity."""
        assert self._handle is not None
        write = StdinWrite(self.session_id, self._handle, number, text)
        await self._execute(self._activities.write_stdin, write, summary)

    async def _watch_output(self) -> None:
        """Run the watch Activity, which Signals the CLI's output until it exits."""
        assert self._handle is not None
        request = WatchRequest(
            self.session_id,
            self._handle,
            self._stream_topic,
            self._options.stderr is not None,
        )
        await self._execute(
            self._activities.watch_output,
            request,
            "Stream output",
            # It runs as long as the CLI, which can wait days on a person, and
            # Temporal needs a finite timeout, so use a year. Heartbeats find a
            # lost Worker, and a retry resumes from the heartbeated offset.
            start_to_close_timeout=timedelta(days=365),
            retry_policy=self._retry_policy or _WATCH_RETRY_POLICY,
        )

    async def _execute(
        self,
        activity: Callable[[_Arg], Awaitable[_Result]],
        arg: _Arg,
        summary: str,
        *,
        cancellation_type: workflow.ActivityCancellationType = workflow.ActivityCancellationType.TRY_CANCEL,
        start_to_close_timeout: timedelta | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> _Result:
        """Run one of the transport's Activities with its options."""
        return await workflow.execute_activity(
            activity,
            arg,
            summary=summary,
            task_queue=self._task_queue,
            start_to_close_timeout=start_to_close_timeout
            or self._start_to_close_timeout,
            heartbeat_timeout=self._heartbeat_timeout,
            retry_policy=retry_policy or self._retry_policy,
            cancellation_type=cancellation_type,
        )

    async def _merge_settings(self) -> None:
        """Merge ``options.sandbox`` into ``options.settings`` in an Activity.

        The Claude Agent SDK does this merge itself, and reads ``settings``
        first if it is a file path. That would be file I/O in the Workflow.
        """
        options = self._options
        if options.sandbox is None or options.settings is None:
            return
        settings = await workflow.execute_activity(
            merge_settings,
            args=[options.settings, options.sandbox],
            summary="Merge settings",
            start_to_close_timeout=timedelta(minutes=1),
        )
        self._options = dataclasses.replace(options, settings=settings, sandbox=None)

    def is_ready(self) -> bool:
        """Whether the transport is connected and not closed."""
        return self._connected and not self._closed

    def _on_output(self, output: CliOutput) -> None:
        """Handle the Signal with a line of the CLI's stdout: take each line once, in order.

        Only this CLI's next line is taken. Anything else is skipped:
        - A line Signalled twice.
        - A line a stale Activity attempt sent early. Its retry sends it again
          in order.
        - A late line from an earlier transport's CLI in the same Workflow.
        """
        if output.session != self.session_id or output.start != self._stdout_offset:
            return
        self._stdout_offset = output.end
        # The same rules as the Claude Agent SDK:
        # - A line over options.max_buffer_size fails.
        # - A line that isn't JSON, such as "[SandboxDebug] ...", is skipped.
        # - A line that starts like JSON but doesn't parse fails.
        try:
            self._check_size(output.line)
            message = _parse_stdout_line(output.line)
        except CLIJSONDecodeError as error:
            self._fail(error)
            return
        if message is None:
            return
        if message.get("type") == "control_response":
            self._on_response(message)
        else:
            self._inbox.append(message)

    def _on_stderr(self, output: CliOutput) -> None:
        """Handle the Signal with a line of the CLI's stderr: as in the Claude Agent SDK, pass it to ``options.stderr``.

        Takes each line of its own CLI once, in order, as :meth:`_on_output` does.
        """
        if output.session != self.session_id or output.start != self._stderr_offset:
            return
        self._stderr_offset = output.end
        line = output.line.rstrip()
        if self._options.stderr is None or not line:
            return
        try:
            self._options.stderr(line)
        except Exception:
            # As in the Claude Agent SDK, a callback that raised misses only this line.
            workflow.logger.debug("stderr callback raised; continuing", exc_info=True)

    def _check_size(self, line: str) -> None:
        """Raise the Claude Agent SDK's error for a line over ``options.max_buffer_size``."""
        limit = self._options.max_buffer_size
        if limit is None:
            limit = _DEFAULT_MAX_BUFFER_SIZE
        if len(line) > limit:
            raise CLIJSONDecodeError(
                f"JSON message exceeded maximum buffer size of {limit} bytes",
                ValueError(f"Buffer size {len(line)} exceeds limit {limit}"),
            )

    def _on_response(self, message: dict[str, Any]) -> None:
        """Hand the CLI's answer to one of the Claude Agent SDK's requests back, under its ID."""
        response = message["response"]
        sdk_id = self._sdk_ids.pop(response["request_id"], None)
        if sdk_id is not None:
            self._inbox.append(
                {**message, "response": {**response, "request_id": sdk_id}}
            )

    async def _receive_output(self) -> None:
        """Watch the CLI's output, even while idle, until it exits. Then read_messages ends.

        Claude Code can start a turn on its own, such as when a background task
        finishes.
        """
        try:
            await self._watch_output()
        except Exception as error:
            self._fail(error)
        finally:
            self._exited = True

    async def _send_input(self) -> None:
        """Write the outbox to the CLI's stdin in order, then the end of input once the Claude Agent SDK ends it."""
        # The stdin writes made, or being made.
        number = 0
        input_ended = False
        while True:
            await workflow.wait_condition(
                lambda: (
                    self._exited
                    or bool(self._outbox)
                    or (self._ended and not input_ended)
                )
            )
            if self._exited:
                return
            number += 1
            try:
                if self._outbox:
                    messages, self._outbox = self._outbox, []
                    text = "".join(f"{json.dumps(message)}\n" for message in messages)
                    kinds: list[str] = [message["type"] for message in messages]
                    labels = dict.fromkeys(_STDIN_LABELS.get(k, k) for k in kinds)
                    await self._write_stdin(number, text, f"Send {', '.join(labels)}")
                else:
                    input_ended = True
                    end = f"{CliFiles.END_OF_INPUT}\n"
                    await self._write_stdin(number, end, "End input")
            except Exception as error:
                self._fail(error)
                return

    def _fail(self, error: BaseException) -> None:
        """Fail the transport, unless it is closed. An Activity that close cancelled is not a failure."""
        if not self._closed and self._failure is None:
            self._failure = _raised(error)


def _raised(error: BaseException) -> BaseException:
    """The error to raise for a failed task.

    An Activity that failed for good becomes the Claude Agent SDK's error. A
    cancellation stays as it is, so Temporal still sees the Workflow as cancelled.
    """
    if isinstance(error, ActivityError) and not is_cancelled_exception(error):
        return sdk_error(error)
    return error
