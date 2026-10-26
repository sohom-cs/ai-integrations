"""Claude Code's command line and environment, built from the Claude Agent SDK's options."""

from __future__ import annotations

import dataclasses

from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk import __version__ as sdk_version
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

# Makes Claude Code emit the session_state_changed messages the Claude Agent SDK
# uses to tell when a run is over.
_READS_SESSION_STATE = "CLAUDE_CODE_SDK_READS_SESSION_STATE"


class ClaudeCodeCommand:
    """As in the Claude Agent SDK, builds Claude Code's command line and environment from its options."""

    def __init__(self, options: ClaudeAgentOptions) -> None:
        """Build from the options passed to ``ClaudeSDKClient``."""
        self._options = options

    def env(self) -> dict[str, str]:
        """As in the Claude Agent SDK, the variables added to the CLI's environment."""
        options = self._options
        env = {
            "CLAUDE_CODE_ENTRYPOINT": "sdk-py",
            **options.env,
            "CLAUDE_AGENT_SDK_VERSION": sdk_version,
        }
        # As in the Claude Agent SDK, set it unless options.env has it, in any
        # letter case.
        if not any(key.upper() == _READS_SESSION_STATE for key in env):
            env[_READS_SESSION_STATE] = "1"
        if options.enable_file_checkpointing:
            env["CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING"] = "true"
        return env

    def args(self) -> list[str]:
        """The CLI's flags."""
        options = self._options
        options = dataclasses.replace(
            options,
            # A placeholder: _build_command raises without a CLI path. Only the
            # flags are used, and the launch Activity finds the real CLI.
            cli_path="claude",
            # With can_use_tool, Claude Code asks for permission over stdio, as
            # in the Claude Agent SDK.
            permission_prompt_tool_name="stdio"
            if options.can_use_tool
            else options.permission_prompt_tool_name,
        )
        # The Claude Agent SDK builds Claude Code's command line only inside its
        # subprocess transport, so that code is borrowed here. Replace this once
        # the Python Claude Agent SDK has a hook like the TypeScript one's
        # spawnClaudeCodeProcess.
        builder = SubprocessCLITransport(prompt="", options=options)
        return builder._build_command()[1:]
