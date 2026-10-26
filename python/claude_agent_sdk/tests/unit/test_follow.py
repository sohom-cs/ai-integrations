"""follow_lines: the lines of a growing file, until Claude Code stops."""

from __future__ import annotations

from temporalio.claude_agent_sdk import follow_lines


async def test_following_yields_whole_lines_from_an_offset() -> None:
    # The first line comes in two writes, and the last has no newline.
    writes = [b'{"n": ', b'1}\n{"n": 2}\nhalf']
    data = bytearray()

    async def running() -> bool:
        if writes:
            data.extend(writes.pop(0))
            return True
        return False

    async def read(offset: int) -> bytes:
        return bytes(data[offset:])

    lines = [line async for line in follow_lines(read, running, 0, poll_interval=0)]

    assert lines == [b'{"n": 1}\n', b'{"n": 2}\n', b"half"]
    from_second = [line async for line in follow_lines(read, running, len(lines[0]))]
    assert from_second == lines[1:]
