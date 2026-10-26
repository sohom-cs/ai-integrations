"""The Signals a :class:`TemporalTransport` gets from the Activity that watches Claude Code's output: their names and payload."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, get_args

# The kinds of line in the CLI's stdout. Each Signal of stdout is named for the
# kind of line it carries, so the Temporal UI shows what it is.
OutputKind = Literal[
    "result",
    "control_request",
    "control_response",
    "tool_use",
    "tool_result",
    "assistant",
    "other",
]
OUTPUT_KINDS: tuple[OutputKind, ...] = get_args(OutputKind)
# The Signal of each line of the CLI's stderr.
STDERR = "stderr"


@dataclass
class CliOutput:
    """A line of Claude Code's stdout or stderr, the Signal a :class:`TemporalTransport` gets for each."""

    # First, so the Temporal UI shows it at the top of the Signal's input.
    summary: str = field(default="", kw_only=True)
    """What the line is, such as ``tool_use: Bash``, for the Temporal UI."""
    start: int
    """The byte offset where the previous Signalled line ended, or 0 for the first.

    Stdout lines between the two were published to the transport's stream.
    """
    end: int
    """The byte offset just past this line's newline."""
    line: str
    """The line, without its newline."""
    session: str = field(kw_only=True)
    """The transport's ``session_id``. A transport takes only its own Claude Code's lines."""
