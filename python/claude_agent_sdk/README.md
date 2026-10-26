# Claude Agent SDK Temporal Integration

> Release stage: [Pre-release](https://docs.temporal.io/develop/python/integrations/claude-agent-sdk).

Run Claude Code agents as Temporal Workflows. As in the Claude Agent SDK, a transport connects
your code to Claude Code, and Temporal Activities carry its input and output.

## Install

```bash
uv add temporalio-claude-agent-sdk
```

## Benefits of Temporal to the Claude Agent SDK

Temporal provides durable orchestration for Claude agents:

- **Durable approvals and hooks.** An agent can pause for a person's approval or your own
  checks and pick up where it left off, however long the wait.
- **Durable tools.** Custom tools you define run as Workflow code and can execute Activities, with
  Temporal's retries and timeouts. A crash doesn't lose their work.
- **Operational visibility with Temporal UI.** See every message, tool call, approval, and
  retry in the UI included with Temporal.

## What's Included

- **`ClaudeAgentSDKPlugin`**: Add it to your Worker to use Claude Code from your Workflows, as you
  would with the Claude Agent SDK: `query()` for one-shot tasks where you know the prompt up front,
  or `ClaudeSDKClient` for multi-turn conversations with follow-ups and interrupts.
- **`TemporalTransport`**: The transport to pass `ClaudeSDKClient` and `query`. It launches Claude
  Code and carries its input and output through the Activities you give it.
- **`subprocess_activities()`**: The transport's default Activities, which run Claude Code in a
  subprocess on the Worker. See [Subprocess Transport](#subprocess-transport).

## How It Works

Claude Code runs whole, as in the Claude Agent SDK, so it keeps all its built-in tools. Your
Workflow drives it through the Claude Agent SDK's usual `ClaudeSDKClient`, and Temporal retries each
step that fails.

- **Activities move the messages.** The transport sends your messages to Claude Code through
  Activities. A long-running Activity follows everything Claude Code writes and sends it to your
  Workflow as Signals.
- **The Activities decide where Claude Code runs.** By default, it runs on the Worker.
  Your own Activities can run it anywhere they can reach.

## Usage

### Basic Setup

#### Workflow Side

Use the Claude Agent SDK's own `ClaudeSDKClient` or `query` in the Workflow, as you would without
Temporal, and pass it a Temporal `transport`.

```python
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, ResultMessage
from temporalio import workflow
from temporalio.claude_agent_sdk import TemporalTransport


@workflow.defn
class ReviewWorkflow:
    @workflow.run
    async def run(self) -> str:
        options = ClaudeAgentOptions()
        transport = TemporalTransport(options)
        async with ClaudeSDKClient(options=options, transport=transport) as client:
            await client.query("Review utils.py for bugs that would cause crashes.")
            async for message in client.receive_response():
                if isinstance(message, ResultMessage):
                    return message.result or ""
        return ""
```

#### Worker Side

Add `ClaudeAgentSDKPlugin` and the Workflow to the Worker.

```python
from temporalio.claude_agent_sdk import ClaudeAgentSDKPlugin
from temporalio.worker import Worker


worker = Worker(
    client,
    task_queue="agents",
    workflows=[ReviewWorkflow],
    plugins=[ClaudeAgentSDKPlugin()],
)
```

## Transports

Always pass `ClaudeSDKClient` and `query` a `transport` in a Workflow. Without one, the Claude
Agent SDK would start Claude Code from Workflow code.

### Subprocess Transport

By default, `TemporalTransport` uses `subprocess_activities()`, the Temporal counterpart of the
Claude Agent SDK's default `SubprocessCLITransport`. They run Claude Code in a subprocess on the
Worker that runs the launch Activity:

- Claude Code runs as the Worker's user, with the Worker's environment, and works in the Worker's
  files. Paths in `ClaudeAgentOptions`, such as `cwd` and `cli_path`, are on the Worker.
- Claude Code runs apart from the Worker process. If only the Worker process restarts, Claude Code
  keeps running, and the next Activity picks up its output where the last one stopped. If the
  container or host restarts, Claude Code stops with it.
- All of the Activities must run on the host Claude Code runs on. If your Workers run on several
  hosts, pass the transport a `task_queue` that only one host polls.

`TemporalTransport` takes options such as `task_queue`, timeouts, and a retry policy:

```python
from temporalio.claude_agent_sdk import TemporalTransport

transport = TemporalTransport(options, task_queue="claude-host-1")
async with ClaudeSDKClient(options=options, transport=transport) as client:
    ...
```

### Custom Transport

To run Claude Code somewhere other than the Worker, such as on a remote host or in a container, write four
Activities and pass them to `TemporalTransport`. The transport runs them; you don't write any
Workflow code.

| Activity | Gets | Does | Returns |
|---|---|---|---|
| `launch` | `LaunchRequest` | Starts Claude Code with `args` and `env`, unless it already started | A handle, such as a host or container ID |
| `write_stdin` | `StdinWrite` | Writes `text` to Claude Code's stdin as write `number` | |
| `watch_output` | `WatchRequest` | Calls `watch_output`, which sends Claude Code's output to the Workflow until it exits | |
| `stop` | `StopRequest` | Stops Claude Code and everything it started | |

Each request has the transport's `session_id`, and every request after `launch` has the `handle`
it returned. Activities can be retried, so make each safe to run more than once.

#### Example

`CliFiles` and `follow_lines` make this easier. `CliFiles` gives the `sh` commands that start, feed, and stop Claude Code,
with its output in files. You run the commands with your host's exec, and read the files with
its file API. `subprocess_activities()` uses the same commands on the Worker.

```python
from collections.abc import AsyncIterator, Callable

from temporalio import activity
from temporalio.claude_agent_sdk import (
    CliFiles,
    LaunchRequest,
    StdinWrite,
    StopRequest,
    WatchRequest,
    follow_lines,
    watch_output,
)


@activity.defn
async def launch_remote(request: LaunchRequest) -> str:
    host = await hosts.get_or_create(request.session_id)
    files = CliFiles("/tmp", request.session_id)
    await host.exec(files.launch_command("claude", request.args), env=request.env)
    return host.id


@activity.defn
async def write_remote(request: StdinWrite) -> None:
    host = await hosts.get(request.handle)
    files = CliFiles("/tmp", request.session_id)
    await host.exec(files.write_stdin_command(request.number), stdin=request.text)


@activity.defn
async def watch_remote(request: WatchRequest) -> None:
    host = await hosts.get(request.handle)
    files = CliFiles("/tmp", request.session_id)

    async def running() -> bool:
        return not await host.exists(files.exit)

    def follow(path: str) -> Callable[[int], AsyncIterator[bytes]]:
        return lambda offset: follow_lines(
            lambda at: host.read(path, at), running, offset, poll_interval=1
        )

    await watch_output(
        request.session_id,
        stdout=follow(files.stdout),
        exit_status=lambda: host.read_exit_status(files.exit),
        stderr=follow(files.stderr),
        stream_topic=request.stream_topic,
        signal_stderr=request.signal_stderr,
    )


@activity.defn
async def stop_remote(request: StopRequest) -> None:
    if request.handle is not None:  # None if launch never finished
        host = await hosts.get(request.handle)
        await host.exec(CliFiles("/tmp", request.session_id).stop_command())
```

Run `launch_command` in a process group of its own, as most exec APIs do, so `stop_command` stops
everything Claude Code started. `follow_lines` turns reads of a growing file into the lines
`watch_output` sends. Here `running` checks only the `exit` file. To also notice when Claude Code
is lost, such as when its host restarts, run `status_command` now and then.

#### Wiring It Up

Group the Activities, then pass them to the transport in the Workflow and to the plugin on the
Worker:

```python
from temporalio.claude_agent_sdk import TemporalTransport, TransportActivities

REMOTE_ACTIVITIES = TransportActivities(
    launch=launch_remote,
    write_stdin=write_remote,
    watch_output=watch_remote,
    stop=stop_remote,
)

# In the Workflow
transport = TemporalTransport(options, REMOTE_ACTIVITIES)
async with ClaudeSDKClient(options=options, transport=transport) as client:
    ...

# On the Worker
ClaudeAgentSDKPlugin(activities=REMOTE_ACTIVITIES)
```

Pass the transport the same `options` as the client.

### Secrets

`ClaudeAgentOptions.env` reaches Claude Code as Activity input, so it is recorded in Workflow
history. Don't put secrets in it.

Set secrets such as `ANTHROPIC_API_KEY` where Claude Code runs instead. By default, Claude Code
inherits the Worker's environment, which never passes through an Activity.

## Callbacks

Approvals, custom tools, and hooks run in your Workflow, so they can use everything a Workflow
can: wait days for a person's input through a Signal or Update, execute Activities with retries
and timeouts, or set a timer. Like all Workflow code, they must be deterministic.

### Tool Approval

Set `can_use_tool` on `ClaudeAgentOptions`, and Claude Code asks it before each tool call, except
tools you pre-approve with `allowed_tools`. In this example, a reviewer lists pending calls with a
Query and decides each with an Update:

```python
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResult,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)
from temporalio import workflow
from temporalio.claude_agent_sdk import TemporalTransport


@workflow.defn
class GovernedAgentWorkflow:
    def __init__(self) -> None:
        self.pending: dict[str, str] = {}
        self.decisions: dict[str, bool] = {}

    @workflow.run
    async def run(self, prompt: str) -> None:
        options = ClaudeAgentOptions(
            tools=["Bash", "Read", "Glob", "Grep"],
            allowed_tools=["Read", "Glob", "Grep"],
            can_use_tool=self.approve,
        )
        transport = TemporalTransport(options)
        async with ClaudeSDKClient(options=options, transport=transport) as client:
            await client.query(prompt)
            async for _ in client.receive_response():
                pass

    async def approve(
        self, tool_name: str, tool_input: dict[str, object], context: ToolPermissionContext
    ) -> PermissionResult:
        tool_use_id = context.tool_use_id or ""
        self.pending[tool_use_id] = tool_name
        await workflow.wait_condition(lambda: tool_use_id in self.decisions)
        del self.pending[tool_use_id]
        if self.decisions[tool_use_id]:
            return PermissionResultAllow()
        return PermissionResultDeny(message=f"{tool_name} was denied")

    @workflow.query
    def pending_tools(self) -> dict[str, str]:
        return self.pending

    @workflow.update
    def decide_tool(self, tool_use_id: str, approved: bool) -> None:
        self.decisions[tool_use_id] = approved
```

### Custom Tools

Pass tools you define with `@tool` and `create_sdk_mcp_server` in `mcp_servers`, and the model
calls them like any other tool.

```python
from datetime import timedelta

from claude_agent_sdk import ClaudeAgentOptions, create_sdk_mcp_server, tool
from temporalio import workflow


@tool("lookup_order", "Look up an order by ID", {"order_id": str})
async def lookup_order(args):
    order = await workflow.execute_activity(
        fetch_order, args["order_id"], start_to_close_timeout=timedelta(seconds=30)
    )
    return {"content": [{"type": "text", "text": order}]}


options = ClaudeAgentOptions(
    mcp_servers={"orders": create_sdk_mcp_server("orders", tools=[lookup_order])},
    allowed_tools=["mcp__orders__lookup_order"],
)
```

### Hooks

Set hook callbacks in `ClaudeAgentOptions.hooks`, for any hook event, and Claude Code calls them
and applies their output.

## Progress

To show callers what the agent is doing while a turn runs, publish its messages to a
[Workflow Stream](https://docs.temporal.io/develop/python/workflows/workflow-streams)
(experimental). Callers subscribe and get each update as it's published:

```python
from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)
from temporalio import workflow
from temporalio.claude_agent_sdk import TemporalTransport
from temporalio.contrib.workflow_streams import WorkflowStream


@workflow.defn
class ProgressWorkflow:
    @workflow.init
    def __init__(self, prompt: str) -> None:
        self.progress = WorkflowStream().topic("progress", type=str)

    @workflow.run
    async def run(self, prompt: str) -> str:
        options = ClaudeAgentOptions()
        transport = TemporalTransport(options)
        async with ClaudeSDKClient(options=options, transport=transport) as client:
            await client.query(prompt)
            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock):
                            self.progress.publish(block.text)
                        elif isinstance(block, ToolUseBlock):
                            self.progress.publish(f"Running {block.name}")
                elif isinstance(message, ResultMessage):
                    return message.result or ""
        return ""
```

A caller subscribes with:

```python
from temporalio.contrib.workflow_streams import WorkflowStreamClient

stream = WorkflowStreamClient.create(client, workflow_id)
async for item in stream.subscribe(["progress"], result_type=str):
    print(item.data)
```

Each message Claude Code writes reaches the Workflow as one Signal, as soon as it's written.

### Token-by-Token Output

To stream Claude's output as it's generated, set `include_partial_messages`, and pass the
transport a `stream` topic of the Workflow's stream. Each `StreamEvent` goes
to that topic for callers outside the Workflow: the Workflow itself doesn't get them, but still
gets every complete message.

```python
from temporalio.claude_agent_sdk import TemporalTransport


@workflow.defn
class StreamingWorkflow:
    @workflow.init
    def __init__(self, prompt: str) -> None:
        self.events = WorkflowStream().topic("events")

    @workflow.run
    async def run(self, prompt: str) -> str:
        options = ClaudeAgentOptions(include_partial_messages=True)
        transport = TemporalTransport(
            options, stream=self.events
        )
        async with ClaudeSDKClient(options=options, transport=transport) as client:
            await client.query(prompt)
            async for message in client.receive_response():
                if isinstance(message, ResultMessage):
                    return message.result or ""
        return ""
```

Callers subscribe to `events` as above; each item's data is a `stream_event` message. A custom
transport takes `stream` the same way. If a Worker fails, a caller may see a few events again. `include_partial_messages` and `stream` go together:
passing one without the other raises `ValueError`.

## Hardening

Consider these additional protections when running Claude Code unattended.

### Restrict access outside the workspace

Configure Claude Code's permission rules and Bash sandbox to restrict access outside the workspace.
Claude's [permissions](https://code.claude.com/docs/en/permissions) control its built-in file tools,
while [sandboxing](https://code.claude.com/docs/en/sandboxing) restricts Bash commands and the
processes they start.

### Run Claude Code outside the Worker

Claude Code can do anything the Worker's user can on its host. For stronger isolation, run Claude
Code in a separate container, VM, or remote environment. Use a
[custom transport](#custom-transport) whose Activities communicate with that environment instead of
launching Claude Code on the Worker.
