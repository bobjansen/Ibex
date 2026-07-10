#!/usr/bin/env bash
# Auto-fix three low-risk, high-signal clang-tidy checks: missing includes
# (misc-include-cleaner), local const-correctness (misc-const-correctness),
# and brace-init field names (modernize-use-designated-initializers). Then
# normalizes const placement to this repo's west-const style (clang-tidy's
# fixit always writes east-const, e.g. `bool const v`), fixes designated-
# initializer spacing/wrapping, and re-sorts any newly-inserted ibex/ headers
# into the angle-bracket project block, since misc-include-cleaner's fixit
# inserts them quoted (e.g. "ibex/ir/node.hpp").
#
# usage: scripts/tidy-fix.sh <files...>
#
# Requires build/compile_commands.json (run the normal cmake -B build config
# first). Only applies fixes for the checks above — this is not a general
# "run all lints" script; see .clang-tidy for the full check list.
#
# misc-const-correctness has a known false-positive pattern: it misses
# mutations that only happen inside a by-reference lambda capture, or inside
# an `if constexpr` branch. Both have been hit in this codebase (window.cpp)
# and would have been compile errors had the fix landed unreviewed. ALWAYS
# rebuild + run tests after running this script, before committing.

set -euo pipefail

usage() {
    cat <<'EOF'
usage: scripts/tidy-fix.sh <files...>

Examples:
  scripts/tidy-fix.sh src/runtime/chunked.cpp
  git diff --name-only | grep -E '\.(cpp|hpp)$' | xargs scripts/tidy-fix.sh
EOF
}

if [[ "$#" -eq 0 ]]; then
    echo "error: no input files given" >&2
    usage >&2
    exit 64
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${TIDY_FIX_BUILD_DIR:-$REPO_ROOT/build}"

if [[ ! -f "$BUILD_DIR/compile_commands.json" ]]; then
    echo "error: $BUILD_DIR/compile_commands.json not found" >&2
    echo "run: cmake -B build -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug" >&2
    exit 1
fi

CLANG_TIDY_BIN="${CLANG_TIDY_BIN:-}"
if [[ -z "$CLANG_TIDY_BIN" ]]; then
    for candidate in clang-tidy-20 clang-tidy-19 clang-tidy-18 clang-tidy; do
        if command -v "$candidate" >/dev/null 2>&1; then
            CLANG_TIDY_BIN="$candidate"
            break
        fi
    done
fi
if [[ -z "$CLANG_TIDY_BIN" ]]; then
    echo "error: clang-tidy not found" >&2
    exit 1
fi

CLANG_FORMAT_BIN="${CLANG_FORMAT_BIN:-}"
if [[ -z "$CLANG_FORMAT_BIN" ]]; then
    for candidate in clang-format-20 clang-format-19 clang-format-18 clang-format; do
        if command -v "$candidate" >/dev/null 2>&1; then
            CLANG_FORMAT_BIN="$candidate"
            break
        fi
    done
fi
if [[ -z "$CLANG_FORMAT_BIN" ]]; then
    echo "error: clang-format not found" >&2
    exit 1
fi

# .clang-format intentionally leaves const placement alone (QualifierAlignment
# defaults to Leave) so it doesn't fight hand-written style elsewhere; add it
# just for this pass, in a scratch copy so the checked-in file is untouched.
STYLE_FILE="$(mktemp)"
trap 'rm -f "$STYLE_FILE"' EXIT
head -n -1 "$REPO_ROOT/.clang-format" > "$STYLE_FILE"  # drop trailing '...'
echo "QualifierAlignment: Left" >> "$STYLE_FILE"
echo "..." >> "$STYLE_FILE"

echo "Fixing includes, const-correctness, and designated initializers in: $*"
# --header-filter='' restricts *fixes* (not just diagnostics) to the main
# file being processed — without it, clang-tidy will silently rewrite
# whatever project headers happen to get transitively included. Deliberately
# NOT --fix-errors: if the TU doesn't compile cleanly, bail out rather than
# apply fixes computed from a broken/error-recovery AST (verified this
# produces flatly wrong fixits, e.g. suggesting `const` on a variable
# mutated two lines later, once the AST was built past a #include error).
"$CLANG_TIDY_BIN" -p "$BUILD_DIR" --header-filter='' \
    --checks='-*,misc-include-cleaner,misc-const-correctness,modernize-use-designated-initializers' \
    --fix "$@"

for f in "$@"; do
    # misc-include-cleaner's fixit inserts ibex/ headers quoted; the project
    # convention is angle brackets for every ibex/ header (see .clang-format's
    # IncludeCategories). Narrow to exactly that prefix so other quoted
    # same-directory includes (e.g. "interpreter_internal.hpp") are untouched.
    sed -i -E 's/#include "(ibex\/[^"]+)"/#include <\1>/' "$f"
    "$CLANG_FORMAT_BIN" -style="file:$STYLE_FILE" -i "$f"
done

echo "Done. Review the diff, then REBUILD AND RUN TESTS before committing —"
echo "misc-const-correctness has real known false positives (see script header)."
