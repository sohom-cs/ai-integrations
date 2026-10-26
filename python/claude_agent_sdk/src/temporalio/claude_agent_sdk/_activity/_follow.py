"""Follow a growing file of Claude Code's output, line by line, wherever it is."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable


async def follow_lines(
    read: Callable[[int], Awaitable[bytes]],
    running: Callable[[], Awaitable[bool]],
    offset: int,
    *,
    poll_interval: float = 0.1,
) -> AsyncIterator[bytes]:
    """Yield the lines of a growing file from byte ``offset`` until Claude Code stops.

    Pass the result to :func:`watch_output` as ``stdout`` or ``stderr``. Each
    line ends in a newline, except possibly the last one, which counts without
    it as in the Claude Agent SDK.

    Args:
        read: Called with a byte offset. Returns the file's bytes from there,
            as many as are handy, or ``b""`` if there are none yet.
        running: Whether Claude Code may still write to the file. Called once
            per poll, so make it cheap, such as by checking for
            :attr:`CliFiles.exit` and running the status command less often.
        offset: Where to start.
        poll_interval: Seconds to wait between polls once there is nothing new.
    """
    # The bytes read after the last newline.
    partial = bytearray()
    while True:
        # Check before reading. If Claude Code stops after this check, the
        # reads below still get everything it wrote.
        still_running = await running()
        while chunk := await read(offset):
            offset += len(chunk)
            *lines, rest = chunk.split(b"\n")
            for line in lines:
                yield bytes(partial) + line + b"\n"
                partial.clear()
            partial += rest
        if not still_running:
            if partial:
                yield bytes(partial)
            return
        await asyncio.sleep(poll_interval)
