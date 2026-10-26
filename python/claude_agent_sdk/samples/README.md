# Samples

A chat with Claude that runs Claude Code on the Worker and asks you before each Bash call. It also
gives Claude a custom `roll_dice` tool that runs an Activity, and asks you before each roll too. It
talks to the real Anthropic API, so it is not part of `make test`; the tests in `../tests` drive the
same code against a scripted API.

- `workflow.py`: `AgentWorkflow`, a `ClaudeSDKClient` conversation in a Workflow.
- `worker.py`: the Worker, which runs Claude Code for each conversation.
- `run.py`: starts a conversation and sends it each line you type.

## Run

Needs Claude Code and the Temporal CLI. Set `ANTHROPIC_API_KEY` or `CLAUDE_CODE_OAUTH_TOKEN` in the
Worker's terminal. Then, from this plugin's directory, run each of these in its own terminal:

```sh
make sync && temporal server start-dev
UV_NO_EDITABLE=1 uv run --locked python -m samples.worker
UV_NO_EDITABLE=1 uv run --locked python -m samples.run
```

