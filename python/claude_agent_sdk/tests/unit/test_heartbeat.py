"""Heartbeating in a transport's Activities."""

from __future__ import annotations

import asyncio

import pytest

from temporalio.claude_agent_sdk import heartbeating
from temporalio.testing import ActivityEnvironment


async def test_a_failed_heartbeat_stops_the_block_and_is_raised() -> None:
    def progress() -> int:
        raise ValueError("No progress")

    async def activity() -> None:
        async with heartbeating(progress):
            await asyncio.sleep(30)

    with pytest.raises(ValueError, match="No progress"):
        await asyncio.wait_for(ActivityEnvironment().run(activity), 5)
