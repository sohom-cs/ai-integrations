# Typescript integrations

See [AGENTS.md](../AGENTS.md) for repository conventions and release procedures.
Install [Bazelisk](https://github.com/bazelbuild/bazelisk); `.bazelversion` pins Bazel and the module graph downloads the language toolchains.

From the repository root:

```bash
bazel test //typescript/google-adk:tests
bazel build //typescript/google-adk:dist
bazel run //typescript/_tools:ci -- conventions
```

Run `bazel test //typescript/google-adk:lint` to check formatting and types.

Each plugin owns its dependencies and lockfiles. Ordinary Bazel builds preserve them. Intentional compatible upgrades use:

```bash
bazel run //typescript/_tools:dev -- update --plugin-dir typescript/google-adk
bazel mod deps --lockfile_mode=update
```

Review the changed manifests and locks before committing. CI performs these updates in a disposable checkout for nightly checks. Cross-plugin dependencies use published coordinates.

Tests use local provider doubles and provision a Temporal server without provider credentials. Temporal CLI archives and checksums are declared in `bazel/temporal.MODULE.bazel`.
Publication artifacts are written under `bazel-bin/typescript/google-adk/dist/`. A protected release version is supplied with `--//bazel:release_version=<version>`; committed development versions remain `0.0.0`.

The native tools in `_tools/` own CI, release validation and artifact checks.
