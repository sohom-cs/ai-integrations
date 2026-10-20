# Java integrations

See [AGENTS.md](../AGENTS.md) for repository conventions and release procedures.
Install [Bazelisk](https://github.com/bazelbuild/bazelisk); `.bazelversion` pins Bazel and the module graph downloads the language toolchains.

From the repository root:

```bash
bazel test //java/spring-ai:tests
bazel build //java/spring-ai:dist
bazel run //java/_tools:ci -- conventions
```

CI checks Java formatting with `bazel run //java/_tools:format -- --dry-run --set-exit-if-changed --plugin-dir java/spring-ai`.

Each plugin owns its dependencies and lockfiles. Ordinary Bazel builds preserve them. Intentional compatible upgrades use:

```bash
bazel run //java/_tools:dev -- update --plugin-dir java/spring-ai
bazel mod deps --lockfile_mode=update
```

Review the changed manifests and locks before committing. CI performs these updates in a disposable checkout for nightly checks. Cross-plugin dependencies use published coordinates.

Tests use local provider doubles and the SDK's in-process Temporal test server without provider credentials.
Publication artifacts are written under `bazel-bin/java/spring-ai/dist/`. A protected release version is supplied with `--//bazel:release_version=<version>`; committed development versions remain `0.0.0`.

The native tools in `_tools/` own CI, release validation and artifact checks.
