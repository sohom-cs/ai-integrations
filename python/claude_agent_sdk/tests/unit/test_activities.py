"""The Activities that run the CLI, run as ActivityEnvironment runs them, with a shell script as the CLI."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from temporalio.claude_agent_sdk import LaunchRequest, StdinWrite
from temporalio.claude_agent_sdk._subprocess._activities import (
    launch,
    write_stdin,
)
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from tests.helpers.harness import CLAUDE, LocalCli
from tests.helpers.workflows import until

pytestmark = pytest.mark.skipif(
    sys.platform == "win32" or not CLAUDE.is_file(),
    reason="needs sh and the Claude Agent SDK's bundled Claude Code",
)


@pytest.fixture
def cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> LocalCli:
    return LocalCli(tmp_path, monkeypatch)


def started_cli(cli: LocalCli, script: str = "exec cat") -> LaunchRequest:
    """The request to launch a CLI that runs ``script``, by default echoing its stdin."""
    cli.set_cli(script)
    return LaunchRequest(str(uuid.uuid4()), [], {}, str(cli.cli_path))


def running(text: str) -> list[str]:
    """The processes whose arguments have ``text``."""
    ps = subprocess.run(
        ["ps", "-A", "-o", "args="], capture_output=True, text=True, check=True
    )
    return [line for line in ps.stdout.splitlines() if text in line]


async def test_a_retried_launch_or_write_starts_and_writes_once(cli: LocalCli) -> None:
    activities = ActivityEnvironment()
    request = started_cli(cli)

    host = await activities.run(launch, request)
    assert await activities.run(launch, request) == host
    first = '{"n": 1}\n{"n": 2}\n'
    write = StdinWrite(request.session_id, host, 1, first)
    await activities.run(write_stdin, write)
    await activities.run(write_stdin, write)
    process = cli.session()
    stdin = Path(process.stdin)
    assert (stdin / "1.jsonl").read_text() == first
    # Attempts that run together, as a retry can with an earlier attempt that
    # outlived it, write the file whole.
    write = StdinWrite(request.session_id, host, 2, '{"n": 3}\n')
    await asyncio.gather(*(activities.run(write_stdin, write) for _ in range(5)))
    assert sorted(path.name for path in stdin.iterdir()) == [
        "1.jsonl",
        "2.jsonl",
    ]
    assert (stdin / "2.jsonl").read_text() == write.text

    # The CLI echoes its stdin: each line, once, in the order of the files.
    await until(lambda: len(Path(process.stdout).read_text().splitlines()) >= 3)
    await asyncio.sleep(0.5)
    lines = Path(process.stdout).read_text().splitlines()
    assert lines == ['{"n": 1}', '{"n": 2}', '{"n": 3}']
    assert len(cli.cli_pids()) == 1
    # A retry after the CLI exited leaves the exit to watching its output.
    await cli.kill_cli_and_wait()
    assert await activities.run(launch, request) == host
    # A write after the CLI exited is no error: watching its output finds the exit.
    await activities.run(write_stdin, StdinWrite(request.session_id, host, 3, "{}\n"))
    # Nothing that fed the CLI's stdin is left running.
    await until(lambda: not running(process.dir))


@pytest.mark.parametrize(
    ("activity", "request_", "reason"),
    [
        (launch, LaunchRequest("s", [], {}, cwd="/missing"), "Working directory"),
        (launch, LaunchRequest("s", [], {}, user="someone"), "options.user"),
        # Claude Code's files are on its host, so a retry elsewhere can't reach them.
        (write_stdin, StdinWrite("s", "elsewhere", 1, "{}\n"), "task_queue"),
    ],
)
async def test_what_no_retry_can_fix_fails_for_good(
    activity: Any, request_: Any, reason: str
) -> None:
    with pytest.raises(ApplicationError) as raised:
        await ActivityEnvironment().run(activity, request_)

    assert raised.value.type == "CLIConnectionError"
    assert raised.value.non_retryable
    assert reason in raised.value.message
