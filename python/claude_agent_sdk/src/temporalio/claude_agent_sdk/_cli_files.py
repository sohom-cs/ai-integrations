"""Claude Code's files wherever it runs, and the shell commands that start, feed, and stop it."""

from __future__ import annotations

import functools
import posixpath
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

_SCRIPTS = Path(__file__).parent / "scripts"


@dataclass(frozen=True)
class CliFiles:
    """Claude Code's stdin, stdout, stderr, and exit status as files in one directory, and the commands that use them.

    The commands need only ``sh``. Run them however you reach the host Claude
    Code runs on, such as a container's exec. To watch the output, read
    :attr:`stdout`, :attr:`stderr`, and :attr:`exit` on that host and pass the
    readers to ``watch_output``. Claude Code outlives each command, and every
    command is safe to retry.
    """

    root: str
    """The directory, on the host Claude Code runs on, to keep its files in."""
    session_id: str
    """The transport's ``session_id``."""

    END_OF_INPUT: ClassVar[str] = "temporal-end-of-input"
    """The line that ends Claude Code's stdin. Write it as the last stdin file."""

    @property
    def dir(self) -> str:
        """The directory of this session's files."""
        return posixpath.join(self.root, f"temporal-claude-code-{self.session_id}")

    @property
    def stdin(self) -> str:
        """The directory of Claude Code's stdin files, fed to it in order: ``1.jsonl``, ``2.jsonl``, and so on."""
        return posixpath.join(self.dir, "stdin")

    @property
    def stdout(self) -> str:
        """The path of Claude Code's stdout."""
        return posixpath.join(self.dir, "stdout.jsonl")

    @property
    def stderr(self) -> str:
        """The path of Claude Code's stderr."""
        return posixpath.join(self.dir, "stderr.log")

    @property
    def exit(self) -> str:
        """The path of Claude Code's exit status, written once it exits."""
        return posixpath.join(self.dir, "exit")

    @property
    def pid(self) -> str:
        """The pid of the shell that runs Claude Code, its process group, and the host's boot ID, once launched."""
        return posixpath.join(self.dir, "pid")

    def launch_command(self, cli_path: str, args: Sequence[str]) -> list[str]:
        """Start Claude Code with ``args``, detached, unless it started already.

        Run it with Claude Code's environment and working directory, in a
        process group of its own, such as with ``setsid``, so
        :meth:`stop_command` stops all Claude Code started.
        """
        return _sh("launch", self.dir, _script("run_cli"), cli_path, *args)

    def write_stdin_command(self, number: int) -> list[str]:
        """Write the command's own stdin as Claude Code's stdin file ``number``. A retry writes the same file again."""
        return _sh("write_stdin", self.dir, str(number))

    def status_command(self) -> list[str]:
        """Print Claude Code's exit status, ``running`` while it may still write output, or ``lost`` if it stopped without one."""
        return _sh("status", self.dir)

    def stop_command(self) -> list[str]:
        """Stop Claude Code and all it started, then remove its files."""
        return _sh("stop", self.dir)


def _sh(name: str, *args: str) -> list[str]:
    """The command that runs ``scripts/<name>.sh`` with ``args``."""
    return ["sh", "-c", _script(name), "sh", *args]


@functools.cache
def _script(name: str) -> str:
    """The text of ``scripts/<name>.sh``, ready for ``sh -c``."""
    common = (_SCRIPTS / "common.sh").read_text()
    text = (_SCRIPTS / f"{name}.sh").read_text()
    return f"end_of_input={CliFiles.END_OF_INPUT}\n{common}{text}"
