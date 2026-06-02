"""Extract the *exact* per-engine code for every benchmark query, straight from
the harness source files.

The whole point is auditability: the benchmark's credibility rests on a skeptic
being able to see precisely what each engine was asked to do (vendor benchmarks
are routinely — and fairly — suspected of hobbling competitors with bad code).
So this module does not hand-maintain a parallel copy of the queries; it parses
the *actual* files that run during the benchmark, which means the rendered code
provably cannot drift from what executed.

Output: extract_all() -> {query_name: {engine: code_string}} plus a sources map.

Source shapes, one per engine family:
  * ibex / ibex+parse  -> {"name", "dsl"...} brace-initialisers in
                          tools/ibex_bench.cpp (C++ adjacent-string literals).
  * pandas/polars       -> run("name", lambda: ...) in bench_python.py,
                          grouped by enclosing bench_<engine>_* function.
  * duckdb/datafusion   -> run("name", lambda: con.sql("...")...) in their files.
  * clickhouse          -> run("name", "<SQL>") string literals.
  * data.table / dplyr  -> bench("fw", "name", function() ...) in bench_r.R.

polars-st is byte-identical to polars (only POLARS_MAX_THREADS differs) and
ibex+parse is byte-identical to ibex (only parse timing differs); both are
aliased onto their twin so the page can annotate rather than duplicate.
"""
from __future__ import annotations

import ast
import os
import re
import textwrap

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# engine -> python source file + the bench_* function-name prefixes that hold
# that engine's queries (bench_python.py mixes pandas and polars).
PY_ENGINES = {
    "pandas": ("benchmarking/bench_python.py", ("bench_pandas",)),
    "polars": ("benchmarking/bench_python.py", ("bench_polars",)),
    "duckdb": ("benchmarking/bench_duckdb.py", ("bench_duckdb",)),
    "datafusion": ("benchmarking/bench_datafusion.py", ("bench_datafusion",)),
    "clickhouse": ("benchmarking/bench_clickhouse.py", ("bench_clickhouse",)),
}
# bench_polars_lazy is a separate (unpublished) variant, not the "polars" engine.
PY_EXCLUDE_FUNCS = {"bench_polars_lazy"}

IBEX_SRC = "tools/ibex_bench.cpp"
R_SRC = "benchmarking/bench_r.R"

SOURCES = {
    "ibex": IBEX_SRC,
    "ibex+parse": IBEX_SRC,
    "pandas": "benchmarking/bench_python.py",
    "polars": "benchmarking/bench_python.py",
    "polars-st": "benchmarking/bench_python.py",
    "duckdb": "benchmarking/bench_duckdb.py",
    "datafusion": "benchmarking/bench_datafusion.py",
    "clickhouse": "benchmarking/bench_clickhouse.py",
    "data.table": R_SRC,
    "dplyr": R_SRC,
}


def _read(rel: str) -> str:
    with open(os.path.join(ROOT, rel), "r") as f:
        return f.read()


def _lambda_body(src_segment: str) -> str:
    """Given the source of a lambda expression, return just its body."""
    s = src_segment.strip()
    if not s.startswith("lambda"):
        return s
    # find the ':' that ends the lambda parameter list (paren/bracket depth 0)
    depth = 0
    for i, ch in enumerate(s):
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == ":" and depth == 0:
            return s[i + 1:].strip()
    return s


def _normalize(code: str) -> str:
    """Strip common leading whitespace from continuation lines so multi-line SQL
    reads cleanly, without altering the first line or any content."""
    lines = code.splitlines()
    if len(lines) <= 1:
        return code.strip()
    tail = lines[1:]
    indents = [len(ln) - len(ln.lstrip()) for ln in tail if ln.strip()]
    cut = min(indents) if indents else 0
    return "\n".join([lines[0]] + [ln[cut:] for ln in tail]).strip()


def _window_helpers(tree: ast.AST) -> dict[str, tuple[str, str]]:
    """Find trivial one-arg helpers like `def w(period): return f"...{period}..."`
    (used to build rolling-window OVER clauses) and return {name: (param, template)}
    where template is the f-string with a literal "{param}" placeholder. Resolving
    these inline makes the actual window frame visible — the crux of the fair
    time-window comparison — instead of an opaque w('60 SECONDS') call."""
    helpers: dict[str, tuple[str, str]] = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.FunctionDef) and len(node.args.args) == 1):
            continue
        body = [s for s in node.body if not isinstance(s, ast.Pass)]
        if len(body) != 1 or not isinstance(body[0], ast.Return):
            continue
        ret = body[0].value
        if isinstance(ret, ast.JoinedStr):
            js = ret
        else:
            continue
        param = node.args.args[0].arg
        parts: list[str] = []
        ok = True
        for v in js.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue) and isinstance(v.value, ast.Name):
                parts.append("{" + v.value.id + "}")
            else:
                ok = False
                break
        if ok and any("{" + param + "}" in p for p in parts):
            helpers[node.name] = (param, "".join(parts))
    return helpers


def _joinedstr_text(node: ast.JoinedStr, src: str, helpers: dict[str, tuple[str, str]]) -> str:
    """Render an f-string SQL payload as plain SQL: literal parts verbatim, and
    {w(...)} window-helper calls expanded; any other interpolation kept as its
    source in braces so nothing is silently hidden."""
    out: list[str] = []
    for v in node.values:
        if isinstance(v, ast.Constant):
            out.append(str(v.value))
        elif isinstance(v, ast.FormattedValue):
            seg = (ast.get_source_segment(src, v.value) or "").strip()
            m = re.fullmatch(r"(\w+)\(([^)]*)\)", seg)
            if m and m.group(1) in helpers:
                param, template = helpers[m.group(1)]
                try:
                    arg = ast.literal_eval(m.group(2).strip())
                    out.append(template.replace("{" + param + "}", str(arg)))
                    continue
                except (ValueError, SyntaxError):
                    pass
            out.append("{" + seg + "}")
    return "".join(out)


def _resolve_calls(code: str, helpers: dict[str, tuple[str, str]]) -> str:
    """Replace {name('arg')} occurrences in an f-string body with the helper's
    expansion, so e.g. {w('60 SECONDS')} becomes the literal OVER (...) clause."""
    if not helpers:
        return code
    pat = re.compile(r"\{(" + "|".join(map(re.escape, helpers)) + r")\(([^)]*)\)\}")

    def sub(m: re.Match) -> str:
        param, template = helpers[m.group(1)]
        try:
            arg = ast.literal_eval(m.group(2).strip())
        except (ValueError, SyntaxError):
            return m.group(0)
        return template.replace("{" + param + "}", str(arg))

    return pat.sub(sub, code)


def _extract_python(rel: str, prefixes: tuple[str, ...]) -> dict[str, str]:
    """Return {query_name: code} for run("name", payload) calls inside any
    top-level function whose name starts with one of `prefixes`."""
    src = _read(rel)
    tree = ast.parse(src)
    helpers = _window_helpers(tree)
    out: dict[str, str] = {}
    for fn in tree.body:
        if not isinstance(fn, ast.FunctionDef):
            continue
        if fn.name in PY_EXCLUDE_FUNCS:
            continue
        if not any(fn.name.startswith(p) for p in prefixes):
            continue
        for node in ast.walk(fn):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == "run" and len(node.args) >= 2):
                continue
            name_node = node.args[0]
            if not (isinstance(name_node, ast.Constant) and isinstance(name_node.value, str)):
                continue
            name = name_node.value
            payload = node.args[1]
            # clickhouse passes SQL directly as a string / f-string -> render the
            # SQL text itself (resolving window helpers), not the f"..." wrapper.
            if isinstance(payload, ast.Constant) and isinstance(payload.value, str):
                code = textwrap.dedent(payload.value).strip()
            elif isinstance(payload, ast.JoinedStr):
                code = _joinedstr_text(payload, src, helpers)
            else:
                seg = ast.get_source_segment(src, payload) or ""
                code = _lambda_body(seg) if isinstance(payload, ast.Lambda) else seg.strip()
            code = _resolve_calls(_normalize(code), helpers)
            # last writer wins is fine: a query is defined once per engine.
            out.setdefault(name, code)
    return out


# Start of a brace-initialiser query: `{ "name" ,` — matches both vector entries
# and `BenchQuery x{"name", ...}` constructions. The DSL value that follows can be
# regular adjacent string literals OR a C++ raw string R"delim(...)delim" (the tf_*
# queries use raw strings so the DSL's own quotes need no escaping), so we parse it
# explicitly rather than with one regex.
_IBEX_HEAD = re.compile(r'\{\s*"(?P<name>[a-z][a-z0-9_]*)"\s*,\s*')


def _parse_cpp_string_value(src: str, i: int) -> str:
    """Parse one or more adjacent C++ string literals (regular or raw) starting at
    src[i], returning the concatenated decoded text."""
    parts: list[str] = []
    n = len(src)
    while i < n:
        while i < n and src[i] in " \t\r\n":
            i += 1
        if i < n and src[i] == "R" and i + 1 < n and src[i + 1] == '"':
            j = i + 2
            paren = src.index("(", j)
            delim = src[j:paren]
            close = ")" + delim + '"'
            end = src.index(close, paren + 1)
            parts.append(src[paren + 1:end])
            i = end + len(close)
        elif i < n and src[i] == '"':
            j = i + 1
            buf: list[str] = []
            while j < n and src[j] != '"':
                if src[j] == "\\":
                    buf.append(src[j:j + 2])
                    j += 2
                else:
                    buf.append(src[j])
                    j += 1
            parts.append("".join(buf).encode().decode("unicode_escape"))
            i = j + 1
        else:
            break
    return "".join(parts)


def _extract_ibex() -> dict[str, str]:
    src = _read(IBEX_SRC)
    out: dict[str, str] = {}
    for m in _IBEX_HEAD.finditer(src):
        name = m.group("name")
        dsl = _parse_cpp_string_value(src, m.end())
        if not dsl:
            continue
        # parse_X is the same DSL timed with parsing; fold onto X.
        if name.startswith("parse_"):
            name = name[len("parse_"):]
        # skip obvious non-query string pairs (heuristic: real queries touch a table)
        if "[" not in dsl and " join " not in dsl:
            continue
        out.setdefault(name, dsl)
    return out


def _scan_call_args(src: str, open_idx: int):
    """From the '(' at open_idx, return (list_of_top_level_arg_strings, end_idx),
    respecting nested (){}[] and string literals."""
    depth = 0
    i = open_idx
    args, start = [], open_idx + 1
    in_str = None
    while i < len(src):
        ch = src[i]
        if in_str:
            if ch == "\\":
                i += 2
                continue
            if ch == in_str:
                in_str = None
        elif ch in "\"'":
            in_str = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                args.append(src[start:i])
                return args, i
        elif ch == "," and depth == 1:
            args.append(src[start:i])
            start = i + 1
        i += 1
    return args, i


def _extract_r() -> dict[str, dict[str, str]]:
    """Return {query_name: {fw: code}} for bench("fw", "name", body) in bench_r.R."""
    src = _read(R_SRC)
    out: dict[str, dict[str, str]] = {}
    for m in re.finditer(r'\bbench\s*\(', src):
        args, _ = _scan_call_args(src, m.end() - 1)
        if len(args) < 3:
            continue
        try:
            fw = ast.literal_eval(args[0].strip())
            name = ast.literal_eval(args[1].strip())
        except (ValueError, SyntaxError):
            continue
        body = ",".join(args[2:]).strip()
        body = re.sub(r"^function\s*\([^)]*\)\s*", "", body).strip()
        out.setdefault(name, {})[fw] = _normalize(body)
    return out


def extract_all() -> dict[str, dict[str, str]]:
    """{query_name: {engine: code_string}}."""
    by_q: dict[str, dict[str, str]] = {}

    def put(q, eng, code):
        by_q.setdefault(q, {})[eng] = code

    for q, code in _extract_ibex().items():
        put(q, "ibex", code)
        put(q, "ibex+parse", code)
    for eng, (rel, prefixes) in PY_ENGINES.items():
        for q, code in _extract_python(rel, prefixes).items():
            put(q, eng, code)
            if eng == "polars":
                put(q, "polars-st", code)
    for q, fws in _extract_r().items():
        for fw, code in fws.items():
            put(q, fw, code)
    return by_q


if __name__ == "__main__":
    import json
    import sys

    data = extract_all()
    if "--coverage" in sys.argv:
        for q in sorted(data):
            print(f"{q:<32} {','.join(sorted(data[q]))}")
        print(f"\n{len(data)} queries")
    else:
        json.dump(data, sys.stdout, indent=2, sort_keys=True)
