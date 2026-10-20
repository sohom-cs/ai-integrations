# Python integrations

See [AGENTS.md](../AGENTS.md) for repository conventions and release procedures.
Install [Bazelisk](https://github.com/bazelbuild/bazelisk); `.bazelversion` pins Bazel and the module graph downloads the language toolchains.

From the repository root:

```bash
bazel test //python/mcp:tests
bazel build //python/mcp:dist
bazel run //python/_tools:ci -- conventions
```

Run `bazel test //python/mcp:lint` to check formatting and types.

Each plugin owns its dependencies and lockfiles. Ordinary Bazel builds preserve them. Intentional compatible upgrades use:

```bash
bazel run //python/_tools:dev -- update --plugin-dir python/mcp
bazel mod deps --lockfile_mode=update
```

Review the changed manifests and locks before committing. CI performs these updates in a disposable checkout for nightly checks. Cross-plugin dependencies use published coordinates.

Tests use local provider doubles and provision a Temporal server without provider credentials. Temporal CLI archives and checksums are declared in `bazel/temporal.MODULE.bazel`.
Publication artifacts are written under `bazel-bin/python/mcp/dist/`. A protected release version is supplied with `--//bazel:release_version=<version>`; committed development versions remain `0.0.0`.

The native tools in `_tools/` own CI, release validation and artifact checks.

Python tests install actual SDK and plugin wheels together and verify RECORD provenance and source equality. No editable installation is used. For an editor environment, run `bazel run //python/_tools:dev -- venv --plugin-dir python/mcp`. Python source stays in each plugin; `_shared/` contains only composable tool configuration. Third-party updates retain the two-week cooldown and first-party exceptions in each pyproject.
