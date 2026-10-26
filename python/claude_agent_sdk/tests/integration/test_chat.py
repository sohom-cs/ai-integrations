"""ClaudeSDKClient in a Workflow, with the bundled Claude Code on the Worker."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest

from temporalio import activity
from temporalio.claude_agent_sdk._subprocess._activities import WATCH_OUTPUT
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
)
from tests.helpers.harness import (
    ANSWERS,
    CLAUDE,
    WRITE_IT_MESSAGES,
    Harness,
    example_chat,
    say,
)
from tests.helpers.workflows import (
    CliRunWorkflow,
    Hook,
    OneShotWorkflow,
    bash,
    tool_use,
    until,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32" or not CLAUDE.is_file(),
    reason="needs sh and the Claude Agent SDK's bundled Claude Code",
)

# What Claude Code tells the model about a call an interrupt stopped.
REJECTED = (
    "The user doesn't want to proceed with this tool use. The tool use was rejected "
    "(eg. if it was a file edit, the new_string was NOT written to the file). STOP "
    "what you are doing and wait for the user to tell you how to proceed."
)


def tool_results(messages: list[Any]) -> list[Any]:
    return [block for m in messages if m[0] == "UserMessage" for block in m[1:]]


def asked_about(messages: list[Any]) -> list[str]:
    return [m[1] for m in messages if m[0] == "Ask"]


def results(messages: list[Any]) -> list[Any]:
    """Each turn's result: its text, and its subtype if it failed."""
    return [m[1:] for m in messages if m[0] == "Result"]


# ANSWERS and "two". "two" first: a turn after an interrupted one has the tool's result too.
WRITE_IT_AND_TWO = {"two": "Two.", **ANSWERS}


def notes(harness: Harness) -> Path:
    """notes.txt in the workspace."""
    return harness.workspace / "notes.txt"


def intercept(
    harness: Harness,
    around: Callable[
        [ExecuteActivityInput, Callable[[], Awaitable[Any]]], Awaitable[Any]
    ],
) -> None:
    """Run each Activity of the harness's Workers through ``around``, which runs it with its second argument."""

    class Inbound(ActivityInboundInterceptor):
        async def execute_activity(self, input: ExecuteActivityInput) -> Any:
            return await around(input, lambda: self.next.execute_activity(input))

    class Intercept(Interceptor):
        def intercept_activity(
            self, next: ActivityInboundInterceptor
        ) -> ActivityInboundInterceptor:
            return Inbound(next)

    harness.interceptors.append(Intercept())


async def test_each_prompt_is_a_turn_of_one_conversation(harness: Harness) -> None:
    answers = {**ANSWERS, "bye": "Bye."}

    messages = await harness.run(answers, ["write it", "bye"])

    # Each of the CLI's messages reaches the Claude Agent SDK once.
    assert [m for m in messages if m[0] != "Ask"] == [
        *WRITE_IT_MESSAGES,
        ["AssistantMessage", ["Bye."]],
        ["Result", "Bye."],
    ]
    assert harness.asked == ["write it", "tool_result:toolu_1", "bye"]
    # One CLI serves both turns.
    assert len(harness.cli_pids()) == 1


async def test_a_denied_tool_does_not_run_and_the_model_reads_why(
    harness: Harness,
) -> None:
    messages = await harness.run(ANSWERS, ["write it"], approve=False)

    assert asked_about(messages) == ["toolu_1"]
    assert tool_results(messages) == [["toolu_1", "A person denied it", True]]
    assert not notes(harness).exists()


async def test_a_lost_watch_of_the_output_goes_on_where_it_was(
    harness: Harness,
) -> None:
    lost: list[str] = []

    async def lose_the_first_watch(
        _watch: ExecuteActivityInput, execute: Callable[[], Awaitable[Any]]
    ) -> Any:
        """Fail the first watch of the CLI's output, as if its Worker were lost, once the API is asked."""
        if activity.info().activity_type != WATCH_OUTPUT or lost:
            return await execute()
        lost.append("watch")
        watching = asyncio.ensure_future(execute())
        await until(lambda: harness.asked)
        await asyncio.sleep(1.5)  # Past a flush of the output.
        watching.cancel()
        await asyncio.wait([watching])
        raise RuntimeError("The Worker was lost")

    intercept(harness, lose_the_first_watch)

    messages = await harness.run(ANSWERS, ["write it"], decide_after=3)

    assert lost
    assert [m for m in messages if m[0] != "Ask"] == WRITE_IT_MESSAGES
    # Nothing started again.
    assert harness.asked == ["write it", "tool_result:toolu_1"]
    assert len(harness.cli_pids()) == 1


async def test_a_cli_that_cannot_run_fails_the_workflow(harness: Harness) -> None:
    harness.set_cli("echo bad flag >&2; exit 2")

    with pytest.raises(WorkflowFailureError) as raised:
        await harness.run({}, ["one"])

    error = raised.value.cause
    assert isinstance(error, ApplicationError)
    assert error.type == "ProcessError"
    assert "bad flag" in error.message
    # Its files are removed.
    assert harness.sessions() == []


async def test_stderr_and_a_settings_file_reach_claude_code_as_in_the_sdk(
    harness: Harness,
) -> None:
    """options.stderr gets each line: without trailing whitespace, blank lines skipped, and the last one even without a newline. options.sandbox merges into a settings file, which an Activity reads."""
    settings = harness.root / "settings.json"
    settings.write_text(json.dumps({"permissions": {"deny": ["Bash"]}}))
    harness.set_cli(
        f'printf "%s\\n" "$@" > "{harness.root}/args"; '
        "echo one >&2; echo >&2; sleep 0.5; printf 'two  ' >&2"
    )

    async with harness.start(
        CliRunWorkflow, str(harness.cli_path), str(settings)
    ) as handle:
        lines = await handle.result()

    assert lines == ["one", "two"]
    args = (harness.root / "args").read_text().splitlines()
    assert json.loads(args[args.index("--settings") + 1]) == {
        "permissions": {"deny": ["Bash"]},
        "sandbox": {"enabled": True},
    }


async def test_the_example_chat_answers_each_message(harness: Harness) -> None:
    harness.answers = ANSWERS
    async with example_chat(harness.client) as handle:
        assert await say(handle, "write it") == "Wrote it."
        assert await say(handle, "read it") == "Read it."
        assert notes(harness).read_text() == "hi\n"
        assert harness.asked == [*ANSWERS]


async def test_a_denial_that_interrupts_ends_the_turn(harness: Harness) -> None:
    messages = await harness.run(
        WRITE_IT_AND_TWO, ["write it", "two"], approve=False, deny_interrupt=True
    )

    assert asked_about(messages) == ["toolu_1"]
    assert results(messages) == [[None, "error_during_execution"], ["Two."]]
    assert harness.asked == ["write it", "two"]
    assert not notes(harness).exists()


def delegate(harness: Harness) -> dict[str, Any]:
    """Answers for "delegate", which runs a foreground subagent whose tool call writes ``ran``."""
    agent = tool_use(
        "toolu_a",
        "Agent",
        description="Write",
        prompt="SUBTASK",
        subagent_type="general-purpose",
        run_in_background=False,
    )
    return {
        "delegate": agent,
        "SUBTASK": bash("toolu_b", f"echo x >> {harness.root / 'ran'}"),
        "tool_result:toolu_b": "Not asked.",
        "tool_result:toolu_a": "Stopped.",
    }


def agent_id(body: dict[str, Any]) -> str:
    """The subagent ID in the request's last tool result, from the Agent call."""
    result = [m for m in body["messages"] if m["role"] == "user"][-1]
    found = re.search(r"agentId: (\w+)", json.dumps(result))
    assert found is not None
    return found.group(1)


async def test_a_denial_that_interrupts_in_a_subagent_stops_the_subagent(
    harness: Harness,
) -> None:
    messages = await harness.run(
        delegate(harness),
        ["delegate"],
        approve=False,
        deny_interrupt=True,
        tools=["Agent", "Bash"],
        allowed_tools=["Agent"],
    )

    assert asked_about(messages) == ["toolu_b"]
    assert not (harness.root / "ran").exists()
    # The subagent asked the model nothing after the denied call; the main turn went on.
    assert harness.asked == ["delegate", "SUBTASK", "tool_result:toolu_a"]
    [[_, _, is_error]] = [b for b in tool_results(messages) if b[0] == "toolu_a"]
    assert is_error
    assert messages[-1] == ["Result", "Stopped."]
    assert len(harness.cli_pids()) == 1


async def test_an_interrupt_while_a_person_decides_ends_the_turn(
    harness: Harness,
) -> None:
    messages = await harness.run(
        WRITE_IT_AND_TWO,
        ["write it", "two"],
        decide_after=3600,
        interrupt_when=lambda: harness.pending,
    )

    assert [
        m for m in messages if m[0] != "AssistantMessage" or m[1][0] != "toolu_1"
    ] == [
        ["Ask", "toolu_1"],
        ["Cancelled", "toolu_1"],
        ["UserMessage", ["toolu_1", REJECTED, True]],
        ["UserMessage", ["[Request interrupted by user for tool use]"]],
        ["Result", None, "error_during_execution"],
        ["AssistantMessage", ["Two."]],
        ["Result", "Two."],
    ]
    assert harness.asked == ["write it", "two"]


def decision(behavior: str, reason: str | None = None) -> dict[str, Any]:
    """A PreToolUse hook's output with a permission decision."""
    output = {"hookEventName": "PreToolUse", "permissionDecision": behavior}
    if reason is not None:
        output["permissionDecisionReason"] = reason
    return {"hookSpecificOutput": output}


async def test_a_hook_deny_refuses_the_call_without_asking(harness: Harness) -> None:
    hooks = [Hook("PreToolUse", decision("deny", "No writes"), matcher="Bash")]

    messages = await harness.run(ANSWERS, ["write it"], hooks=hooks)

    assert asked_about(messages) == []
    denied = "PreToolUse:Bash hook error: No writes"
    assert tool_results(messages) == [["toolu_1", denied, True]]
    assert not notes(harness).exists()
    [hook_input] = harness.hook_inputs
    assert hook_input["tool_use_id"] == "toolu_1"


async def test_a_hook_that_raised_has_no_opinion(harness: Harness) -> None:
    hook = Hook("PreToolUse", decision("deny", "No."), fail=True)

    messages = await harness.run(ANSWERS, ["write it"], hooks=[hook])

    # The call is asked about, as if there were no hook.
    assert asked_about(messages) == ["toolu_1"]
    assert notes(harness).exists()


async def test_a_hook_out_of_time_stops_the_call(harness: Harness) -> None:
    hook = Hook("PreToolUse", decision("allow"), wait=5, timeout=1)

    messages = await harness.run(ANSWERS, ["write it"], hooks=[hook])

    # As in Claude Code, the call does not run.
    assert asked_about(messages) == []
    [[_, text, is_error]] = tool_results(messages)
    assert is_error
    assert "did not respond before its timeout" in text
    assert not notes(harness).exists()


async def test_an_interrupt_while_a_hook_runs_ends_the_turn(harness: Harness) -> None:
    hooks = [Hook("PreToolUse", decision("allow"), wait=3600)]

    messages = await harness.run(
        WRITE_IT_AND_TWO,
        ["write it", "two"],
        interrupt_when=lambda: harness.hook_inputs,
        hooks=hooks,
    )

    assert results(messages) == [[None, "error_during_execution"], ["Two."]]
    assert not notes(harness).exists()
    assert len(harness.cli_pids()) == 1


async def test_an_sdk_mcp_tool_runs_once_in_the_workflow(harness: Harness) -> None:
    answers = {
        "echo it": tool_use("toolu_1", "mcp__app__echo", text="hi"),
        "tool_result:toolu_1": "Echoed.",
    }

    messages = await harness.run(answers, ["echo it"], sdk_tools=True)

    # It is asked about as Claude Code asks about an MCP tool.
    assert [m for m in messages if m[0] in ("Ask", "Echo")] == [
        ["Ask", "toolu_1"],
        ["Echo", "hi"],
    ]
    assert tool_results(messages) == [
        ["toolu_1", [{"type": "text", "text": "echo: hi"}], None]
    ]
    assert messages[-1] == ["Result", "Echoed."]
    assert harness.asked == ["echo it", "tool_result:toolu_1"]


def background(command: str) -> dict[str, Any]:
    """Answers for "start it", which runs ``command`` in the background; its end wakes the agent."""
    return {
        # First: the harness answers with the first key the last user message
        # has, and the notification's text has the other keys too.
        "<task-notification>": "Finished.",
        "start it": tool_use(
            "toolu_1", "Bash", command=command, run_in_background=True
        ),
        "tool_result:toolu_1": "Started.",
    }


async def test_the_sdks_query_function_ends_once_claude_code_is_done(
    harness: Harness,
) -> None:
    """As without Temporal, a background task finishes first, and its end starts another turn."""
    harness.answers = background("sleep 2")
    async with harness.start(OneShotWorkflow, "start it", ["Bash"]) as handle:
        messages = await handle.result()

    assert results(messages) == [["Started."], ["Finished."]]
    # Ending the input stopped the CLI, and its files are removed.
    assert harness.sessions() == []


async def test_a_turn_claude_code_starts_on_its_own_while_idle_is_watched(
    harness: Harness,
) -> None:
    """A background task's end starts a turn, whose tool call asks the Workflow."""
    answers = {
        **background("sleep 2"),
        "<task-notification>": bash("toolu_2", "echo hi > notes.txt"),
        "tool_result:toolu_2": "Finished.",
    }

    messages = await harness.run(answers, ["start it"], follow_ups=1)

    assert asked_about(messages) == ["toolu_2"]
    assert results(messages) == [["Started."], ["Finished."]]
    assert len(harness.cli_pids()) == 1
