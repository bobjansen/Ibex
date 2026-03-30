from __future__ import annotations

import argparse
import shlex
import sys
from pathlib import Path

from IPython.core.error import UsageError
from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from IPython.display import display


def repo_root() -> Path:
    return Path(__file__).resolve().parent


def add_bridge_module_path() -> None:
    root = repo_root()
    for build_dir_name in ("build-release", "build"):
        candidate = root / build_dir_name / "python"
        if candidate.is_dir():
            path = str(candidate)
            if path not in sys.path:
                sys.path.insert(0, path)
            return
    raise RuntimeError("could not find a built ibex_pyarrow module under build-release/python or build/python")


add_bridge_module_path()

import ibex_pyarrow


def default_plugin_paths() -> list[str]:
    root = repo_root()
    paths: list[str] = []
    for build_dir_name in ("build-release", "build"):
        for relative in ("tools", "libraries"):
            candidate = root / build_dir_name / relative
            if candidate.is_dir():
                paths.append(str(candidate))
    return paths


def _build_parser(prog: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog, add_help=False)
    parser.add_argument("--as", dest="result_format", choices=("pyarrow", "pandas"), default="pyarrow")
    parser.add_argument("--out", default=None, help="store the result in this Python variable")
    parser.add_argument(
        "--bind",
        action="append",
        default=[],
        metavar="IBEX=PYTHON",
        help="bind a Python table-like object into the Ibex script",
    )
    parser.add_argument("--quiet", action="store_true", help="do not display the resulting table")
    parser.add_argument("-h", "--help", action="store_true")
    return parser


def _build_file_parser() -> argparse.ArgumentParser:
    parser = _build_parser("%ibexfile")
    parser.add_argument("path")
    return parser


def _parse_args(line: str, prog: str) -> argparse.Namespace:
    parser = _build_parser(prog)
    try:
        args = parser.parse_args(shlex.split(line))
    except SystemExit as exc:
        raise UsageError(f"{prog}: invalid arguments") from exc
    if args.help:
        raise UsageError(parser.format_help().rstrip())
    return args


def _parse_file_args(line: str) -> argparse.Namespace:
    parser = _build_file_parser()
    try:
        args = parser.parse_args(shlex.split(line))
    except SystemExit as exc:
        raise UsageError("%ibexfile: invalid arguments") from exc
    if args.help:
        raise UsageError(parser.format_help().rstrip())
    return args


def _resolve_bindings(shell, bind_specs: list[str]) -> dict[str, object]:
    bindings: dict[str, object] = {}
    for spec in bind_specs:
        if "=" not in spec:
            raise UsageError(f"--bind expects IBEX=PYTHON, got: {spec}")
        ibex_name, python_name = spec.split("=", 1)
        ibex_name = ibex_name.strip()
        python_name = python_name.strip()
        if not ibex_name or not python_name:
            raise UsageError(f"--bind expects IBEX=PYTHON, got: {spec}")
        if python_name not in shell.user_ns:
            raise UsageError(f"Python variable not found for binding: {python_name}")
        bindings[ibex_name] = shell.user_ns[python_name]
    return bindings


def _convert_result(result, result_format: str):
    if result_format == "pandas":
        return result.to_pandas()
    return result


@magics_class
class IbexMagics(Magics):
    @cell_magic
    def ibex(self, line: str, cell: str):
        args = _parse_args(line, "%%ibex")
        bindings = _resolve_bindings(self.shell, args.bind)
        result = ibex_pyarrow.eval_table(
            cell, tables=bindings or None, plugin_paths=default_plugin_paths()
        )
        converted = _convert_result(result, args.result_format)
        target_name = args.out or "_ibex"
        self.shell.user_ns[target_name] = converted
        if not args.quiet:
            display(converted)
        return converted

    @line_magic
    def ibexfile(self, line: str):
        args = _parse_file_args(line)
        bindings = _resolve_bindings(self.shell, args.bind)
        result = ibex_pyarrow.eval_file(
            args.path, tables=bindings or None, plugin_paths=default_plugin_paths()
        )
        converted = _convert_result(result, args.result_format)
        target_name = args.out or "_ibex"
        self.shell.user_ns[target_name] = converted
        if not args.quiet:
            display(converted)
        return converted


def load_ipython_extension(ipython) -> None:
    ipython.register_magics(IbexMagics)
