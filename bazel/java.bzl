"""Bazel Java publications with separately locked compilation and runtime graphs."""

load("@bazel_skylib//rules:common_settings.bzl", "BuildSettingInfo")
load("@rules_java//java:defs.bzl", "JavaInfo")

def _dist_impl(ctx):
    out = ctx.actions.declare_directory("dist")
    jars = ctx.attr.library[JavaInfo].transitive_compile_time_jars
    args = ctx.actions.args()
    args.add_all(["build", "--project", ctx.label.package, "--version", ctx.attr._version[BuildSettingInfo].value, "--spring-boot", ctx.attr._boot[BuildSettingInfo].value, "--jar", ctx.attr.library[JavaInfo].outputs.jars[0].class_jar.path, "--output", out.path])
    args.add_joined("--classpath", jars, join_with = ctx.configuration.host_path_separator)
    ctx.actions.run(executable = ctx.executable._tool, inputs = depset(ctx.files.srcs, transitive = [ctx.attr.library[DefaultInfo].files, jars]), tools = [ctx.attr._tool[DefaultInfo].files_to_run], outputs = [out], arguments = [args], mnemonic = "MavenDistribution")
    return [DefaultInfo(files = depset([out]))]

java_dist = rule(implementation = _dist_impl, attrs = {
    "library": attr.label(providers = [JavaInfo]),
    "srcs": attr.label_list(allow_files = True),
    "_tool": attr.label(default = "//java/_tools:package", cfg = "exec", executable = True),
    "_version": attr.label(default = "//bazel:release_version"),
    "_boot": attr.label(default = "//bazel:spring_boot"),
})
