#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Run the clang-tidy version CI runs: scripts/clang-tidy-version holds it,
# and the CI clang-tidy job installs that same version. Findings differ between
# versions, so a local run on any other version proves nothing about CI.
# CLANG_TIDY_BIN overrides the executable (a path, or another version on
# purpose).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLANG_TIDY_VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/scripts/clang-tidy-version")"

CLANG_TIDY_BIN="${CLANG_TIDY_BIN:-}"
if [[ -z "$CLANG_TIDY_BIN" ]]; then
    if command -v "clang-tidy-$CLANG_TIDY_VERSION" >/dev/null 2>&1; then
        CLANG_TIDY_BIN="clang-tidy-$CLANG_TIDY_VERSION"
    elif command -v clang-tidy >/dev/null 2>&1 &&
        clang-tidy --version | grep -q "version $CLANG_TIDY_VERSION\."; then
        CLANG_TIDY_BIN="clang-tidy"
    fi
fi

if [[ -z "$CLANG_TIDY_BIN" ]]; then
    echo "error: clang-tidy $CLANG_TIDY_VERSION not found (the version CI runs, from" >&2
    echo "       scripts/clang-tidy-version). Install it from apt.llvm.org:" >&2
    echo "         wget -qO /tmp/llvm.sh https://apt.llvm.org/llvm.sh" >&2
    echo "         sudo bash /tmp/llvm.sh $CLANG_TIDY_VERSION && sudo apt-get install -y clang-tidy-$CLANG_TIDY_VERSION" >&2
    echo "       or set CLANG_TIDY_BIN to run another version deliberately." >&2
    exit 1
fi

# Default the compile database to the repo's Debug build dir, so
# `scripts/clang-tidy.sh <files>` works without `-p`. An explicit -p wins, and
# arguments after `--` mean the caller supplied the compile flags themselves.
needs_build_dir=1
for arg in "$@"; do
    case "$arg" in
        -p | --p | -p=* | --p=* | -- | --version | -version | --help | -help | --list-checks | -list-checks)
            needs_build_dir=0
            break
            ;;
    esac
done

if [[ "$needs_build_dir" -eq 1 ]]; then
    if [[ ! -f "$REPO_ROOT/build/compile_commands.json" ]]; then
        echo "error: $REPO_ROOT/build/compile_commands.json not found; configure build/ or pass -p <build-dir>" >&2
        exit 1
    fi
    set -- -p "$REPO_ROOT/build" "$@"
fi

# A header has no entry in the compile database, so clang-tidy run on one
# borrows the flags of an unrelated entry (without -std=c++23, std::format
# does not exist). CI never lints a header directly: it reports a header's
# findings while linting each src/*.cpp that includes it (--header-filter).
# Do the same here: lint a translation unit that includes the header, and
# report only the header's own diagnostics.
# Options (with the value after -p) apply to every run; files are split into
# translation units, linted together as given, and headers, each linted alone.
args=()
units=()
headers=()
expect_value=false
after_dashes=false
for arg in "$@"; do
    if [[ "$after_dashes" == true || "$arg" == "--" ]]; then
        args+=("$arg")  # compile flags after `--`
        after_dashes=true
        continue
    fi
    if [[ "$expect_value" == true ]]; then
        args+=("$arg")
        expect_value=false
        continue
    fi
    case "$arg" in
        -p | --p) args+=("$arg"); expect_value=true; continue ;;
        -*) args+=("$arg"); continue ;;
        *.hpp | *.h)
            if [[ -f "$arg" ]]; then
                headers+=("$(cd "$(dirname "$arg")" && pwd)/$(basename "$arg")")
                continue
            fi
            ;;
    esac
    units+=("$arg")
done

if [[ ${#headers[@]} -eq 0 ]]; then
    exec "$CLANG_TIDY_BIN" "${args[@]}" "${units[@]}"
fi

# The translation unit to lint a header through: a src/ .cpp that includes it,
# spelled either <ibex/...> (public headers) or by file name (internal ones).
unit_for_header() {
    local header="$1" spelling unit
    if [[ "$header" == "$REPO_ROOT/include/"* ]]; then
        spelling="${header#"$REPO_ROOT/include/"}"
    else
        spelling="$(basename "$header")"
    fi
    unit="$(grep -rlF --include='*.cpp' "$spelling" "$REPO_ROOT/src" |
        xargs -r grep -lE "^#include [<\"](.*/)?${spelling//./\\.}[>\"]" 2>/dev/null | head -1 || true)"
    printf '%s' "$unit"
}

status=0
if [[ ${#units[@]} -gt 0 ]] && ! "$CLANG_TIDY_BIN" "${args[@]}" "${units[@]}"; then
    status=1
fi
for header in "${headers[@]}"; do
    unit="$(unit_for_header "$header")"
    if [[ -z "$unit" ]]; then
        echo "error: no src/*.cpp includes $header directly; lint a translation unit that does" >&2
        status=1
        continue
    fi
    echo "clang-tidy: $header (through ${unit#"$REPO_ROOT/"})" >&2
    escaped="$(printf '%s' "$header" | sed 's/[.[\*^$()+?{}|]/\\&/g')"
    # line-filter limits reporting to the header, so the unit's own findings
    # stay out of a header lint.
    if ! "$CLANG_TIDY_BIN" "${args[@]}" "--header-filter=^${escaped}\$" \
        "--line-filter=[{\"name\":\"$header\"}]" "$unit"; then
        status=1
    fi
done
exit "$status"
