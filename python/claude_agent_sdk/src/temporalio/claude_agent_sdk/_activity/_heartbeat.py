"""Heartbeats for a transport's Activities, so Temporal soon notices a lost Worker."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from typing import Any

from temporalio import activity


@contextlib.asynccontextmanager
async def heartbeating(
    progress: Callable[[], Any] | None = None,
) -> AsyncGenerator[None, None]:
    """Heartbeat every second while the block runs.

    Use it in an Activity that can take longer than its heartbeat timeout,
    such as one that starts or stops Claude Code. If a heartbeat fails, the
    block is cancelled and the failure is raised.

    Args:
        progress: Called before each heartbeat. Its result is the heartbeat's
            details, which a retry of the Activity reads from
            ``activity.info().heartbeat_details``.
    """
    block = asyncio.current_task()
    assert block is not None

    async def heartbeat() -> None:
        try:
            while True:
                if progress is None:
                    activity.heartbeat()
                else:
                    activity.heartbeat(progress())
                await asyncio.sleep(1)
        except Exception:
            # Cancel the block now. The finally below then raises the failure.
            block.cancel()
            raise

    heartbeats = asyncio.create_task(heartbeat())
    try:
        yield
    finally:
        heartbeats.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeats
