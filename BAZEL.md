# Bazel development

Install [Bazelisk](https://github.com/bazelbuild/bazelisk). Run commands from the repository root; `.bazelversion` pins Bazel. No Make installation is required.

```bash
bazel test //...
bazel build //python/mcp:dist
bazel test //typescript/google-adk:tests //typescript/google-adk:lint
bazel test //java/spring-ai:tests --config=java17 --//bazel:spring_boot=4.1.1
bazel test //go/googleadk:tests --config=go126
```

The common plugin targets are `:build`, `:tests`, and `:dist`. Python and TypeScript also have `:lint`; Go has a formatting test plus `bazel run //go/_tools:lint -- --plugin-dir go/googleadk` for the selected SDK's vet. Java formatting uses `bazel run //java/_tools:format -- --dry-run --set-exit-if-changed --plugin-dir java/spring-ai`.

Distribution files appear under `bazel-bin/<language>/<plugin>/dist/`. Release builds pass `--//bazel:release_version=<version>` without modifying committed development versions. CI copies the designated cell's tested bytes to the existing release artifact names. Registry integrity, clean consumer installation, immutable tags and publication recovery remain release requirements.

Each plugin owns its native manifest and lock. `python/_deps.MODULE.bazel`, `typescript/_deps.MODULE.bazel`, `java/_deps.MODULE.bazel` and `go/_deps.MODULE.bazel` expose independent graphs. Cross-plugin imports resolve published packages. Java separately locks compilation, test compilation and test runtime for each Spring Boot version. `MODULE.bazel.lock` pins the Bazel dependency graph; ordinary builds use `--lockfile_mode=error`.

Intentional compatible upgrades use the relevant language's developer tool:

```bash
bazel run //<language>/_tools:dev -- update --plugin-dir <language>/<plugin>
bazel mod deps --lockfile_mode=update
```

Review manifest and native lock changes, then the Bazel lock. Maven graphs can be repinned explicitly with `REPIN=1 bazel run @<graph>//:pin --lockfile_mode=update`. New plugins also declare an independent dependency repository in their language's module fragment. Never add a root language workspace to make dependency conflicts disappear.

For a Python editor environment, use `bazel run //python/_tools:dev -- venv --plugin-dir python/mcp`. Integration tests install the actual SDK and plugin wheels together and check RECORD provenance. The shared Python directory contains configuration only.

`bazel/temporal.MODULE.bazel` declares checksum-verified CLI archives for macOS, Linux and Windows, including the existing Python CLI/server version. Tests receive executable paths through runfiles. Java uses the SDK's in-process test server. The pinned Gradle distribution only tests Maven consumer installation; it does not build plugins.

Runtime configurations are `py310`, `py311`, `py314`, `node22`, `node24`, `java17`, `java25`, `go126` and `go127`. CI retains Ubuntu min/max and macOS/Windows max, plus every Java compatibility variant. `.github/actions/setup-bazel` uses commit-scoped caches and only saves on trusted main pushes. Optional local settings belong in ignored `user.bazelrc`.

The [Temporal agent harness](https://github.com/temporal-community/temporal-agent-harness/tree/578acdc04c75e3cc6bb91d9985f720265aea3bdf) informed the CLI runfiles, runtime configuration and cache setup. This repository retains independent package locks, native publication artifacts, and Windows coverage.
