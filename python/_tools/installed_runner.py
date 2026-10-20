"""Run checks against an unpacked installed wheel, never the plugin's source imports."""

from __future__ import annotations

import argparse
import importlib
import os
import shutil
import subprocess
import sys
import tempfile
import venv
import zipfile
from pathlib import Path

from python.runfiles import runfiles


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--wheel", required=True)
    parser.add_argument("--sdk-wheel", required=True)
    parser.add_argument("--extra-wheel", action="append", default=[])
    parser.add_argument("--mode", choices=["test", "lint", "package"], required=True)
    args = parser.parse_args()
    resolver = runfiles.Create()

    def locate(value: str) -> Path:
        path = Path(value)
        if path.is_absolute() or path.is_file():
            return path.absolute()
        resolved = resolver.Rlocation(value)
        if not resolved:
            raise FileNotFoundError(f"Missing Bazel runfile: {value}")
        return Path(resolved)

    root = Path(resolver.Rlocation("_main/" + args.project + "/pyproject.toml")).parent
    wheel = locate(args.wheel)
    server = os.environ.get("TEMPORAL_CLI_PATH")
    node = os.environ.get("NODE_BINARY")
    if node:
        os.environ["PATH"] = (
            str(locate(node).parent) + os.pathsep + os.environ.get("PATH", "")
        )
    if server:
        os.environ["TEMPORAL_CLI_PATH"] = str(locate(server))
    with tempfile.TemporaryDirectory(
        prefix="installed-plugin-", dir=os.environ.get("TEST_TMPDIR")
    ) as directory:
        environment = Path(directory) / "venv"
        venv.EnvBuilder(with_pip=False, symlinks=os.name != "nt").create(environment)
        if os.name == "nt":
            installed = environment / "Lib" / "site-packages"
            interpreter = environment / "Scripts" / "python.exe"
        else:
            installed = (
                environment
                / "lib"
                / f"python{sys.version_info.major}.{sys.version_info.minor}"
                / "site-packages"
            )
            interpreter = environment / "bin" / "python"
        scripts = environment / ("Scripts" if os.name == "nt" else "bin")
        dependency_paths = [
            Path(path)
            for path in sys.path
            if Path(path).name == "site-packages" and Path(path).is_dir()
        ]
        # Merge the independently locked installed wheels into one environment,
        # matching a normal installation for regular and namespace packages.
        for dependency_path in dependency_paths:
            shutil.copytree(dependency_path, installed, dirs_exist_ok=True)
            binaries = dependency_path.parent / "bin"
            if binaries.is_dir():
                for binary in binaries.iterdir():
                    if binary.is_file():
                        shutil.copy2(binary, scripts / binary.name)
        os.environ["PATH"] = str(scripts) + os.pathsep + os.environ.get("PATH", "")
        # The interpreter is also used by xdist and provider subprocesses, whose
        # deliberately restricted environments may omit PYTHONPATH.
        sys.executable = str(interpreter)
        os.environ["VIRTUAL_ENV"] = str(environment)
        for dependency in [args.sdk_wheel, *args.extra_wheel]:
            with zipfile.ZipFile(locate(dependency)) as sdk:
                sdk.extractall(installed)
        with zipfile.ZipFile(wheel) as archive:
            for entry in archive.infolist():
                if (
                    Path(entry.filename).is_absolute()
                    or ".." in Path(entry.filename).parts
                ):
                    raise ValueError("wheel contains an unsafe path")
                if ".data" in entry.filename.split("/")[0]:
                    raise ValueError(
                        "integration wheels must not contain wheel .data paths"
                    )
            archive.extractall(installed)
        sys.path.insert(0, str(installed))
        sys.path.insert(1, str(root))
        # The SDK extends its regular package path to discover independently installed plugins.
        importlib.invalidate_caches()
        os.environ["PYTHONPATH"] = os.pathsep.join(sys.path)
        os.chdir(root)
        if args.mode == "test":
            import pytest

            os.environ.setdefault(
                "PYTEST_XDIST_AUTO_NUM_WORKERS", str(min(os.cpu_count() or 1, 4))
            )

            return pytest.main(
                ["-c", "pyproject.toml", "-n", "auto", "--dist=worksteal", "tests"]
            )
        if args.mode == "package":
            import importlib.metadata as metadata

            from tests.helpers.plugin_meta import load_plugin_meta
            from tests.helpers.provenance import check_provenance

            meta = load_plugin_meta(root)
            check_provenance(
                meta.coordinate,
                meta.package_relpath,
            )
            importlib.import_module(meta.root_api)
            assert metadata.distribution(meta.coordinate).version in wheel.name
            return 0
        commands = [
            ["ruff", "check", "--select", "I"],
            ["ruff", "format", "--check"],
            ["pyright", "--pythonpath", sys.executable],
            ["mypy"],
            ["basedpyright", "--pythonpath", sys.executable],
            ["pydocstyle", "--ignore-decorators=overload", "src"],
        ]
        for module, *arguments in commands:
            if module == "ruff":
                executable = scripts / ("ruff.exe" if os.name == "nt" else "ruff")
                command = [str(executable), *arguments]
            else:
                command = [sys.executable, "-m", module, *arguments]
            result = subprocess.run(command, check=False)
            if result.returncode:
                return result.returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
