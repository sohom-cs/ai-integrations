import asyncio
import sys
from collections.abc import AsyncIterable
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from mcp import StdioServerParameters, stdio_client
from strands.tools.mcp.mcp_client import MCPClient
from strands.types.streaming import StreamEvent

from temporalio import workflow
from temporalio.client import Client
from temporalio.strands_agents import (
    StrandsPlugin,
    TemporalAgent,
    TemporalMCPClient,
    _temporal_mcp_client,
)
from temporalio.strands_agents._temporal_mcp_tool import TemporalMCPTool
from temporalio.worker import Replayer, Worker
from tests.common import get_activities
from tests.mock_model import MockModel


def _echo_client_factory() -> MCPClient:
    return MCPClient(
        lambda: stdio_client(
            StdioServerParameters(
                command=sys.executable,
                args=[str(Path(__file__).parent / "echo_mcp_server.py")],
            )
        )
    )


@workflow.defn
class MCPWorkflow:
    def __init__(self) -> None:
        echo = TemporalMCPClient(
            server="echo",
            cache_tools=True,
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.agent = TemporalAgent(
            model="mock",
            start_to_close_timeout=timedelta(seconds=30),
            tools=[echo],
        )

    @workflow.run
    async def run(self, prompt: str) -> str:
        result = await self.agent.invoke_async(prompt)
        return str(result)


async def test_mcp(client: Client):
    task_queue = f"test_mcp-{uuid4()}"
    plugin = StrandsPlugin(
        models={
            "mock": lambda: MockModel(
                [
                    {"name": "echo", "input": {"message": "hello"}},
                    "Done!",
                ]
            )
        },
        mcp_clients={
            "echo": lambda: MCPClient(
                lambda: stdio_client(
                    StdioServerParameters(
                        command=sys.executable,
                        args=[str(Path(__file__).parent / "echo_mcp_server.py")],
                    )
                )
            ),
        },
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[MCPWorkflow],
        plugins=[plugin],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            MCPWorkflow.run,
            "echo hello",
            id=f"test_mcp_{uuid4()}",
            task_queue=task_queue,
        )
        assert await handle.result() == "Done!\n"

    history = await handle.fetch_history()
    assert get_activities(history) == [
        "echo-list-tools",
        "invoke_model",
        "echo-call-tool",
        "invoke_model",
    ]

    await Replayer(
        workflows=[MCPWorkflow],
        plugins=[plugin],
    ).replay_workflow(history)


@workflow.defn
class MCPReuseWorkflow:
    def __init__(self) -> None:
        echo = TemporalMCPClient(
            server="echo_cached",
            cache_tools=True,
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.agent = TemporalAgent(
            model="mock",
            start_to_close_timeout=timedelta(seconds=30),
            tools=[echo],
        )

    @workflow.run
    async def run(self, prompt: str) -> str:
        result = await self.agent.invoke_async(prompt)
        return str(result)


async def test_mcp_reuses_connection(client: Client):
    """Successive MCP tool calls reuse one cached worker-side connection."""
    task_queue = f"test_mcp_reuses_connection-{uuid4()}"
    # Count how often the worker opens a connection. One lazily-opened
    # connection serves the list-tools discovery and both tool calls (1);
    # reconnecting per call would make it more.
    factory_calls = [0]

    def counting_factory() -> MCPClient:
        factory_calls[0] += 1
        return _echo_client_factory()

    plugin = StrandsPlugin(
        models={
            "mock": lambda: MockModel(
                [
                    {"name": "echo", "input": {"message": "one"}},
                    {"name": "echo", "input": {"message": "two"}},
                    "Done!",
                ]
            )
        },
        mcp_clients={"echo_cached": counting_factory},
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[MCPReuseWorkflow],
        plugins=[plugin],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            MCPReuseWorkflow.run,
            "echo twice",
            id=f"test_mcp_reuses_connection_{uuid4()}",
            task_queue=task_queue,
        )
        assert await handle.result() == "Done!\n"

    # The worker context has exited, so its run_context finally evicted the
    # cached connection.
    assert "echo_cached" not in _temporal_mcp_client._CONNECTIONS
    assert factory_calls[0] == 1

    history = await handle.fetch_history()
    assert get_activities(history) == [
        "echo_cached-list-tools",
        "invoke_model",
        "echo_cached-call-tool",
        "invoke_model",
        "echo_cached-call-tool",
        "invoke_model",
    ]

    await Replayer(
        workflows=[MCPReuseWorkflow],
        plugins=[plugin],
    ).replay_workflow(history)


@workflow.defn
class MCPIdleWorkflow:
    def __init__(self) -> None:
        echo = TemporalMCPClient(
            server="echo_idle",
            cache_tools=True,
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.agent = TemporalAgent(
            model="mock",
            start_to_close_timeout=timedelta(seconds=30),
            tools=[echo],
        )

    @workflow.run
    async def run(self, prompt: str) -> str:
        result = await self.agent.invoke_async(prompt)
        return str(result)


async def test_mcp_connection_idle_timeout(client: Client):
    """A short idle timeout evicts the cached connection while the worker runs."""
    task_queue = f"test_mcp_connection_idle_timeout-{uuid4()}"
    factory_calls = [0]

    def counting_factory() -> MCPClient:
        factory_calls[0] += 1
        return _echo_client_factory()

    plugin = StrandsPlugin(
        models={
            "mock": lambda: MockModel(
                [
                    {"name": "echo", "input": {"message": "hello"}},
                    "Done!",
                ]
            )
        },
        mcp_clients={"echo_idle": counting_factory},
        # Short window so the cached call connection is evicted mid-run rather
        # than only at worker shutdown. The idle timer only arms once the call
        # releases the connection, so this can't tear it down mid-call.
        mcp_connection_idle_timeout=timedelta(milliseconds=100),
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[MCPIdleWorkflow],
        plugins=[plugin],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            MCPIdleWorkflow.run,
            "echo hello",
            id=f"test_mcp_connection_idle_timeout_{uuid4()}",
            task_queue=task_queue,
        )
        assert await handle.result() == "Done!\n"

        # A connection was opened lazily (on the first list-tools/call-tool).
        # How many times depends on whether the short idle timer fires between
        # activities, so this only asserts that at least one was opened; the
        # eviction-while-alive behavior is what the polling loop below checks.
        assert factory_calls[0] >= 1

        # Still inside the worker context: the short idle timer evicts the
        # cached call connection on its own. Asserting eviction here -- with the
        # worker alive -- proves it came from the idle timer, not shutdown.
        for _ in range(100):
            if "echo_idle" not in _temporal_mcp_client._CONNECTIONS:
                break
            await asyncio.sleep(0.1)
        assert "echo_idle" not in _temporal_mcp_client._CONNECTIONS


@workflow.defn
class MCPNoCacheWorkflow:
    def __init__(self) -> None:
        echo = TemporalMCPClient(
            server="echo_nocache",
            cache_tools=False,
            start_to_close_timeout=timedelta(seconds=30),
        )
        self.agent = TemporalAgent(
            model="mock",
            start_to_close_timeout=timedelta(seconds=30),
            tools=[echo],
        )

    @workflow.run
    async def run(self, prompt: str) -> str:
        result = await self.agent.invoke_async(prompt)
        return str(result)


async def test_mcp_lists_tools_each_turn_when_uncached(client: Client):
    """With cache_tools=False the tool list is re-fetched on every model call."""
    task_queue = f"test_mcp_lists_tools_each_turn_when_uncached-{uuid4()}"
    plugin = StrandsPlugin(
        models={
            "mock": lambda: MockModel(
                [
                    {"name": "echo", "input": {"message": "one"}},
                    {"name": "echo", "input": {"message": "two"}},
                    "Done!",
                ]
            )
        },
        mcp_clients={"echo_nocache": _echo_client_factory},
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[MCPNoCacheWorkflow],
        plugins=[plugin],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            MCPNoCacheWorkflow.run,
            "echo twice",
            id=f"test_mcp_lists_tools_each_turn_when_uncached_{uuid4()}",
            task_queue=task_queue,
        )
        assert await handle.result() == "Done!\n"

    history = await handle.fetch_history()
    activities = get_activities(history)
    # One list-tools per model call -- the tools are re-listed every turn rather
    # than once for the workflow.
    assert activities.count("echo_nocache-list-tools") == activities.count(
        "invoke_model"
    )
    assert activities.count("echo_nocache-list-tools") == 3

    await Replayer(
        workflows=[MCPNoCacheWorkflow],
        plugins=[plugin],
    ).replay_workflow(history)


def _echo_client_with_prefix(prefix: str) -> MCPClient:
    return MCPClient(
        lambda: stdio_client(
            StdioServerParameters(
                command=sys.executable,
                args=[str(Path(__file__).parent / "echo_mcp_server.py")],
            )
        ),
        prefix=prefix,
    )


_seen_tool_names: list[list[str]] = []


class ToolNameRecordingModel(MockModel):
    """MockModel that records the tool names offered on each call."""

    async def stream(self, *args: Any, **kwargs: Any) -> AsyncIterable[StreamEvent]:
        tool_specs = args[1] if len(args) > 1 else kwargs.get("tool_specs")
        _seen_tool_names.append(sorted(spec["name"] for spec in tool_specs or []))
        async for event in super().stream(*args, **kwargs):
            yield event


@workflow.defn
class MCPTwoServersWorkflow:
    def __init__(self) -> None:
        self.agent = TemporalAgent(
            model="mock",
            start_to_close_timeout=timedelta(seconds=30),
            tools=[
                TemporalMCPClient(
                    server="echo_a", start_to_close_timeout=timedelta(seconds=30)
                ),
                TemporalMCPClient(
                    server="echo_b", start_to_close_timeout=timedelta(seconds=30)
                ),
            ],
        )

    @workflow.run
    async def run(self, prompt: str) -> str:
        try:
            result = await self.agent.invoke_async(prompt)
        except ValueError as err:
            return f"refused: {err}"
        return str(result)


async def test_mcp_prefix_names_the_tool(client: Client):
    """A prefixed server's tool keeps its prefix and calls its own server."""
    task_queue = f"test_mcp_prefix_names_the_tool-{uuid4()}"
    _seen_tool_names.clear()
    plugin = StrandsPlugin(
        models={
            "mock": lambda: ToolNameRecordingModel(
                [{"name": "b_echo", "input": {"message": "hi"}}, "Done!"]
            )
        },
        mcp_clients={
            "echo_a": _echo_client_factory,
            "echo_b": lambda: _echo_client_with_prefix("b"),
        },
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[MCPTwoServersWorkflow],
        plugins=[plugin],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            MCPTwoServersWorkflow.run,
            "echo hi",
            id=f"test_mcp_prefix_names_the_tool_{uuid4()}",
            task_queue=task_queue,
        )
        assert await handle.result() == "Done!\n"

    assert _seen_tool_names[0] == ["b_echo", "echo"]
    history = await handle.fetch_history()
    assert "echo_b-call-tool" in get_activities(history)
    assert "echo_a-call-tool" not in get_activities(history)

    await Replayer(
        workflows=[MCPTwoServersWorkflow],
        plugins=[plugin],
    ).replay_workflow(history)


async def test_mcp_duplicate_tool_names_are_refused(client: Client):
    """Two servers that list the same tool name on one agent raise, as in Strands."""
    task_queue = f"test_mcp_duplicate_tool_names_are_refused-{uuid4()}"
    plugin = StrandsPlugin(
        models={"mock": lambda: MockModel(["Done!"])},
        mcp_clients={"echo_a": _echo_client_factory, "echo_b": _echo_client_factory},
    )

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[MCPTwoServersWorkflow],
        plugins=[plugin],
        max_cached_workflows=0,
    ):
        handle = await client.start_workflow(
            MCPTwoServersWorkflow.run,
            "echo hi",
            id=f"test_mcp_duplicate_tool_names_are_refused_{uuid4()}",
            task_queue=task_queue,
        )
        assert (await handle.result()).startswith(
            "refused: Tool name 'echo' already exists."
        )

    await Replayer(
        workflows=[MCPTwoServersWorkflow],
        plugins=[plugin],
    ).replay_workflow(await handle.fetch_history())


def _mcp_tool(server: str, name: str, agent_name: str | None = None) -> TemporalMCPTool:
    info = _temporal_mcp_client._MCPToolInfo(
        name=name, description="", input_schema={}, agent_name=agent_name
    )
    return TemporalMCPTool(server, info, {})


async def _run_hook(hook: Any, event: Any) -> None:
    """Run a BeforeModelCallEvent hook that TemporalAgent registered."""
    await hook(event)


def _listing(provider: TemporalMCPClient, tools: list[TemporalMCPTool]) -> Any:
    """Stand in for one {server}-list-tools result, as _refresh() stores it."""

    async def refresh() -> None:
        provider._tools = list(tools)
        provider._fetched = True

    return refresh


@pytest.mark.parametrize("cache_tools", [False, True])
async def test_mcp_duplicate_refused_again_after_caught(
    monkeypatch: pytest.MonkeyPatch, cache_tools: bool
):
    """Catching the duplicate-name error does not let the next turn take the name."""
    monkeypatch.setattr(workflow, "patched", lambda _id: True)
    a = TemporalMCPClient("a", cache_tools=cache_tools)
    c = TemporalMCPClient("c", cache_tools=cache_tools)
    agent = TemporalAgent(model="m", tools=[a, c])
    event = SimpleNamespace(agent=agent)
    hook_a = agent._make_mcp_refresh_hook(a)
    hook_c = agent._make_mcp_refresh_hook(c)
    monkeypatch.setattr(a, "_refresh", _listing(a, [_mcp_tool("a", "echo")]))
    monkeypatch.setattr(c, "_refresh", _listing(c, [_mcp_tool("c", "echo")]))

    for _turn in range(2):
        await _run_hook(hook_a, event)
        with pytest.raises(ValueError, match="Tool name 'echo' already exists"):
            await _run_hook(hook_c, event)
        assert agent.tool_registry.registry["echo"]._server == "a"  # type: ignore[attr-defined]


async def test_mcp_upgrade_keeps_other_servers_tool(monkeypatch: pytest.MonkeyPatch):
    """A run that let b replace a's `echo` keeps a's `echo` when b gains its prefix."""
    # A run recorded before the duplicate check: the gate keeps its replacing behavior.
    monkeypatch.setattr(workflow, "patched", lambda _id: False)
    a = TemporalMCPClient("a")
    b = TemporalMCPClient("b")
    agent = TemporalAgent(model="m", tools=[a, b])
    event = SimpleNamespace(agent=agent)
    hook_a = agent._make_mcp_refresh_hook(a)
    hook_b = agent._make_mcp_refresh_hook(b)

    # Before the upgrade both servers listed `echo`, and b's replaced a's.
    monkeypatch.setattr(a, "_refresh", _listing(a, [_mcp_tool("a", "echo")]))
    monkeypatch.setattr(b, "_refresh", _listing(b, [_mcp_tool("b", "echo")]))
    await _run_hook(hook_a, event)
    await _run_hook(hook_b, event)
    assert agent.tool_registry.registry["echo"]._server == "b"  # type: ignore[attr-defined]

    # After the upgrade b's listing carries its prefix.
    monkeypatch.setattr(b, "_refresh", _listing(b, [_mcp_tool("b", "echo", "b_echo")]))
    await _run_hook(hook_a, event)
    await _run_hook(hook_b, event)
    registry = agent.tool_registry.registry
    assert sorted(registry) == ["b_echo", "echo"]
    assert registry["echo"]._server == "a"  # type: ignore[attr-defined]
    assert registry["b_echo"]._server == "b"  # type: ignore[attr-defined]
