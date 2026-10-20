"""Native TypeScript compilation, AVA tests, ESLint and npm distribution actions."""

load("@aspect_rules_js//js:defs.bzl", "js_library", "js_test")
load("@aspect_rules_js//js:providers.bzl", "JsInfo")
load("@aspect_rules_ts//ts:defs.bzl", "ts_config", "ts_project")
load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")
load("@plugin_metadata//:plugins.bzl", "PLUGINS")

def _dist_impl(ctx):
    version = ctx.attr._version[BuildSettingInfo].value
    output = ctx.actions.declare_file("dist/" + ctx.attr.coordinate.replace("@", "").replace("/", "-") + "-" + version + ".tgz")
    args = ctx.actions.args()
    args.add_all(["pack", "--project", ctx.label.package, "--compiled", ctx.bin_dir.path + "/" + ctx.label.package + "/lib", "--version", version, "--output", output.path])
    inputs = depset(ctx.files.srcs, transitive = [target[DefaultInfo].files for target in ctx.attr.compiled] + [target[JsInfo].types for target in ctx.attr.compiled])
    ctx.actions.run(executable = ctx.executable._packer, arguments = [args], inputs = inputs, tools = [ctx.attr._packer[DefaultInfo].files_to_run], outputs = [output], env = {"BAZEL_BINDIR": ctx.bin_dir.path, "JS_BINARY__NO_CD_BINDIR": "1"}, mnemonic = "NpmDistribution")
    return [DefaultInfo(files = depset([output]))]

npm_dist = rule(implementation = _dist_impl, attrs = {
    "srcs": attr.label_list(allow_files = True),
    "compiled": attr.label_list(),
    "coordinate": attr.string(),
    "_packer": attr.label(default = "//typescript/_tools:package", cfg = "exec", executable = True),
    "_version": attr.label(default = "//bazel:release_version"),
})

def typescript_integration(name = "check"):
    """Declare native TypeScript integration targets.

    Args:
        name: Name of the aggregate check suite.
    """
    package = native.package_name()
    manifest = PLUGINS[package]["project"]
    helpers = ["//" + package + "/tests/helpers:build"] if "@temporalio/test-helpers" in manifest.get("devDependencies", {}) else []
    js_library(name = "runtime_sources", srcs = ["package.json"] + native.glob(["src/**", "tests/smoke/**"], allow_empty = True))
    ts_config(name = "tsconfig", src = "tsconfig.json", deps = [":node_modules/@tsconfig/node20", "//typescript/_shared:tsconfig"])
    ts_project(name = "build", srcs = native.glob(["src/**/*.ts"]), tsconfig = ":tsconfig", out_dir = "lib", root_dir = "src", composite = True, incremental = True, source_map = True, deps = [":node_modules"] + helpers)
    js_test(name = "tests", entry_point = "//typescript/_tools:lib/test-runner.js", data = [":build", ":node_modules", "//bazel/temporal:cli", "//typescript/_tools:test_runtime", ":tsconfig", ":runtime_sources"], no_copy_to_bin = ["//bazel/temporal:cli"], chdir = package, env = {"TEMPORAL_CLI_RUNFILE": "$(rlocationpath //bazel/temporal:cli)"}, tags = ["no-remote-exec"], size = "large", timeout = "long")
    js_test(name = "lint", entry_point = "//typescript/_tools:lib/lint-runner.js", data = [":build", ":node_modules", ":tsconfig", ":runtime_sources", "//typescript/_tools:lint_runtime", "//typescript/_shared:config"], chdir = package)
    npm_dist(name = "dist", coordinate = manifest["name"], compiled = [":build"], srcs = native.glob(["src/**", "README.md", "LICENSE", "package.json"]))
    native.test_suite(name = name, tests = [":lint", ":tests"])
