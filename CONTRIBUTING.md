# Contributing

Read [`AGENTS.md`](AGENTS.md) first; it is the normative guide and this page only summarizes the
workflow.

## Set up

Install [Bazelisk](https://github.com/bazelbuild/bazelisk). From the repository root:

```bash
bazel test //<language>/<plugin>:tests
bazel build //<language>/<plugin>:dist
```

Bazel downloads pinned language toolchains and consumes each plugin's independent lockfile. See each language README for lint, editor environment and intentional dependency-update targets.

## Pull requests

- PRs are squash-merged. Issue and PR templates are the organization defaults from
  `temporalio/.github`.
- CI runs only the plugins whose files changed (`.github/workflows/ci.yml`); `ci-status`,
  `Check for CODEOWNERS` and `opengrep/scan` are the required checks, plus one code-owner
  approval. No secrets are needed: provider behavior uses deterministic local test doubles.

## Adding a Python plugin

```bash
bazel run //python/_tools:new_plugin -- <name> --description "..." --maturity pre-release
```

The scaffolder copies `python/_template`, prints the remaining manual steps, and no workflow
edits are needed: CI discovers plugins from `plugin.toml`.

## Contributor License Agreement

All contributors must complete the Temporal Contributor License Agreement (CLA)
before changes can be merged. A link to the CLA will be posted in the pull request.
