"""The errors a transport's Activities fail with, and the Claude Agent SDK errors a :class:`TemporalTransport` raises for them."""

from __future__ import annotations

from claude_agent_sdk import (
    ClaudeSDKError,
    CLIConnectionError,
    CLINotFoundError,
    ProcessError,
)

from temporalio.exceptions import ActivityError, ApplicationError


def process_error(status: int, stderr: bytes) -> ApplicationError:
    """The error for Claude Code exiting with a nonzero status. As in the Claude Agent SDK, the transport raises ``ProcessError`` for it."""
    return ApplicationError(
        f"Command failed with exit code {status}",
        status,
        stderr.decode(errors="replace"),
        type=ProcessError.__name__,
        non_retryable=True,
    )


def cli_not_found_error(message: str) -> ApplicationError:
    """The error for an Activity that finds no Claude Code to run. Raise it to fail the Activity for good.

    As in the Claude Agent SDK, the transport raises ``CLINotFoundError`` for it.
    """
    return ApplicationError(message, type=CLINotFoundError.__name__, non_retryable=True)


def cli_connection_error(message: str) -> ApplicationError:
    """The error for an Activity that can't reach or start Claude Code. Raise it to fail the Activity for good.

    As in the Claude Agent SDK, the transport raises ``CLIConnectionError`` for it.
    """
    return ApplicationError(
        message, type=CLIConnectionError.__name__, non_retryable=True
    )


def sdk_error(error: ActivityError) -> ClaudeSDKError:
    """The error for an Activity that failed for good. As in the Claude Agent SDK, it is a ``ClaudeSDKError``."""
    cause = error.cause
    if not isinstance(cause, ApplicationError):
        sdk: ClaudeSDKError = CLIConnectionError(str(cause or error))
    elif cause.type == ProcessError.__name__:
        status, stderr = cause.details
        sdk = ProcessError(cause.message, status, stderr)
    elif cause.type == CLINotFoundError.__name__:
        sdk = CLINotFoundError(cause.message)
    else:
        sdk = CLIConnectionError(cause.message)
    sdk.__cause__ = error
    return sdk
