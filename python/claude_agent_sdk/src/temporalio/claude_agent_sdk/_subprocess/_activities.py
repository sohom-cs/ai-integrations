"""Activities that run Claude Code in a subprocess on the Worker's host.

Like the Claude Agent SDK's ``SubprocessCLITransport``, but through
:class:`CliFiles`' commands. Claude Code runs as the Worker's user, with the Worker's environment, so
isolate the Worker for production. Paths in the options, such as ``cwd`` and
``cli_path``, are on the Worker.

Each Activity attempt can run alongside an earlier one that outlived it, so
every Activity must be safe to retry and to run concurrently. Claude Code's
files are on its host only, so every Activity of a session must run there.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from pathlib import Path

from claude_agent_sdk import ClaudeAgentOptions, CLINotFoundError
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

from temporalio import activity

from .._activity import _watch
from .._activity._follow import follow_lines
from .._activity._heartbeat import heartbeating
from .._cli_files import CliFiles
from .._errors import cli_connection_error, cli_not_found_error
from .._transport import (
    LaunchRequest,
    StdinWrite,
    StopRequest,
    TransportActivities,
    WatchRequest,
)

LAUNCH = "claude-code-launch"
WRITE_STDIN = "claude-code-write-stdin"
WATCH_OUTPUT = "claude-code-watch-output"
CLOSE = "claude-code-close"

# How often following the output asks whether Claude Code was lost.
_STATUS_INTERVAL = 5.0


@activity.defn(name=LAUNCH)
async def launch(request: LaunchRequest) -> str:
    """Start Claude Code, unless it started already, and return this host's name.

    As in the Claude Agent SDK, first check the working directory, find Claude
    Code, and check its version. ``options.user`` isn't supported: Claude Code
    runs as the Worker's user.
    """
    async with heartbeating():
        if request.user is not None:
            raise cli_connection_error(
                "Claude Code runs as the Worker's user; run the Worker as that "
                "user instead of setting options.user"
            )
        files = _files(request.session_id, None)
        cwd = request.cwd
        if cwd is not None and not os.path.isdir(cwd):
            raise cli_connection_error(f"Working directory does not exist: {cwd}")
        cli_path = await _find_cli(request.cli_path, cwd)
        # Same as the Claude Agent SDK. Start from the Worker's environment
        # minus CLAUDECODE, which marks a nested Claude Code. Add the options'
        # variables, then PWD.
        env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
        env.update(request.env)
        if cwd is not None:
            env["PWD"] = cwd
        await _run(files.launch_command(cli_path, request.args), env=env, cwd=cwd)
        return socket.gethostname()


@activity.defn(name=WRITE_STDIN)
async def write_stdin(write: StdinWrite) -> None:
    """Write text as one of Claude Code's stdin files."""
    files = _files(write.session_id, write.handle)
    await _run(files.write_stdin_command(write.number), input=write.text.encode())


@activity.defn(name=WATCH_OUTPUT)
async def watch_output(request: WatchRequest) -> None:
    """Send Claude Code's output to the Workflow as Signals until it exits."""
    files = _files(request.session_id, request.handle)
    await _watch.watch_output(
        request.session_id,
        lambda offset: _follow(files, files.stdout, offset),
        lambda: _exit_status(files),
        lambda offset: _follow(files, files.stderr, offset),
        stream_topic=request.stream_topic,
        signal_stderr=request.signal_stderr,
    )


@activity.defn(name=CLOSE)
async def close(request: StopRequest) -> None:
    """Stop Claude Code and everything it started, and remove its files."""
    async with heartbeating():
        await _run(_files(request.session_id, request.handle).stop_command())


def subprocess_activities() -> TransportActivities:
    """The default Activities: run Claude Code in a subprocess on the Worker.

    As the Claude Agent SDK's ``SubprocessCLITransport`` does. Claude Code runs
    as the Worker's user, with the Worker's environment. All the Activities
    must run on the host Claude Code runs on, so with Workers on more than one
    host, pass the transport a ``task_queue`` only one host polls.
    """
    return TransportActivities(launch, write_stdin, watch_output, close)


def _follow(files: CliFiles, path: str, offset: int) -> AsyncIterator[bytes]:
    """Follow one of Claude Code's output files on this host."""

    async def read(offset: int) -> bytes:
        def read_chunk() -> bytes:
            with open(path, "rb") as file:
                file.seek(offset)
                return file.read(1 << 16)

        return await asyncio.to_thread(read_chunk)

    return follow_lines(read, _running(files), offset)


def _running(files: CliFiles) -> Callable[[], Awaitable[bool]]:
    """A cheap check of whether Claude Code may still write output.

    Its exit file shows a normal exit at once. The status command, which also
    finds Claude Code lost, runs only every few seconds.
    """
    asked = time.monotonic()

    async def running() -> bool:
        nonlocal asked
        if os.path.exists(files.exit):
            return False
        if time.monotonic() - asked < _STATUS_INTERVAL:
            return True
        asked = time.monotonic()
        return (await _run(files.status_command())).strip() == b"running"

    return running


def _files(session_id: str, cli_host: str | None) -> CliFiles:
    """The session's files in this host's temp dir. ``cli_host`` is the launch Activity's handle: the host Claude Code runs on.

    Raises:
        ApplicationError: If Claude Code runs on another host.
    """
    host = socket.gethostname()
    if cli_host is not None and cli_host != host:
        raise cli_connection_error(
            f"Claude Code runs on {cli_host}, but this Activity ran on {host}. "
            "Pass TemporalTransport a task_queue that only one host polls."
        )
    return CliFiles(tempfile.gettempdir(), session_id)


async def _exit_status(files: CliFiles) -> int | None:
    """Claude Code's exit status, or ``None`` if it was killed with the shell that runs it."""
    try:
        # Written aside and moved into place, so never empty.
        return int(Path(files.exit).read_text())
    except FileNotFoundError:
        return None


async def _run(
    command: Sequence[str],
    *,
    input: bytes | None = None,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
) -> bytes:
    """Run one of :class:`CliFiles`' commands on this host, and return its stdout.

    It runs in a new session, so Claude Code gets a process group of its own.
    :meth:`CliFiles.stop_command` signals that group, and Ctrl-C on the Worker
    doesn't reach it.

    Raises:
        RuntimeError: If the command failed.
    """
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.DEVNULL if input is None else asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
        env=env,
        start_new_session=True,
    )
    stdout, stderr = await process.communicate(input)
    if process.returncode:
        raise RuntimeError(stderr.decode(errors="replace"))
    return stdout


async def _find_cli(cli_path: str | None, cwd: str | None) -> str:
    """The CLI to run: ``cli_path`` if set. Otherwise, as in the Claude Agent SDK, the bundled CLI, then ``claude`` on the ``PATH``.

    A relative ``cli_path`` such as ``./claude`` is in ``cwd``, where the CLI runs.

    Raises:
        ApplicationError: If there is no such CLI.
    """
    if cli_path is not None and cwd is not None and os.sep in cli_path:
        cli_path = os.path.join(cwd, cli_path)
    if cli_path is None:
        try:
            cli_path = await asyncio.to_thread(_sdk_transport(None)._find_cli)
        except CLINotFoundError as error:
            raise cli_not_found_error(str(error)) from None
    elif shutil.which(cli_path) is None:
        # sh runs the CLI, so a missing one wouldn't raise FileNotFoundError.
        # Check for it now.
        raise cli_not_found_error(f"Claude Code not found at: {cli_path}")
    if not os.environ.get("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK"):
        await _sdk_transport(cli_path)._check_claude_version()
    return cli_path


def _sdk_transport(cli_path: str | None) -> SubprocessCLITransport:
    """The Claude Agent SDK's transport, to borrow how it finds and checks Claude Code."""
    return SubprocessCLITransport(
        prompt="", options=ClaudeAgentOptions(cli_path=cli_path)
    )
