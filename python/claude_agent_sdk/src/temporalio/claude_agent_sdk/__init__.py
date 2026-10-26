"""Claude Agent SDK on Temporal."""

from __future__ import annotations

from temporalio.claude_agent_sdk._activity._follow import follow_lines
from temporalio.claude_agent_sdk._activity._heartbeat import heartbeating
from temporalio.claude_agent_sdk._activity._watch import watch_output
from temporalio.claude_agent_sdk._cli_files import CliFiles
from temporalio.claude_agent_sdk._errors import (
    cli_connection_error,
    cli_not_found_error,
)
from temporalio.claude_agent_sdk._plugin import ClaudeAgentSDKPlugin
from temporalio.claude_agent_sdk._signals import CliOutput
from temporalio.claude_agent_sdk._subprocess._activities import subprocess_activities
from temporalio.claude_agent_sdk._transport import (
    LaunchRequest,
    StdinWrite,
    StopRequest,
    TemporalTransport,
    TransportActivities,
    WatchRequest,
)

__all__ = [
    "ClaudeAgentSDKPlugin",
    "CliFiles",
    "CliOutput",
    "LaunchRequest",
    "StdinWrite",
    "StopRequest",
    "TemporalTransport",
    "TransportActivities",
    "WatchRequest",
    "cli_connection_error",
    "cli_not_found_error",
    "follow_lines",
    "heartbeating",
    "subprocess_activities",
    "watch_output",
]
