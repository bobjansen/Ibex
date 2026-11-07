#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# clang-format wrapper: choose the newest available clang-format.

set -euo pipefail

usage() {
    cat <<'EOF'
usage: scripts/clang-format.sh [clang-format args] <files...>
       scripts/clang-format.sh -r [clang-format args] [paths...]

  -r, --recursive   format every C/C++ source (.c .cc .cpp .cxx .h .hh .hpp
                    .hxx .ipp .inl) under each path, or under the repository
                    root when no path is given. Inside a git work tree only
                    files git would track are taken (tracked, or untracked and
                    not ignored), so build trees and fetched dependencies are
                    skipped. `-ir` / `-ri` are `-i -r`. In this mode every
                    argument that does not start with `-` is a path, so pass
                    clang-format options in their `--opt=value` form.

Examples:
  scripts/clang-format.sh -i src/core/foo.cpp
  scripts/clang-format.sh -ir                    # the whole repository, in place
  scripts/clang-format.sh -ir src/runtime tests  # just these trees
  scripts/clang-format.sh -r --dry-run --Werror  # check only, nonzero on drift
  echo 'int main(){return 0;}' | scripts/clang-format.sh
EOF
}

recursive=0
args=()
for arg in "$@"; do
    case "$arg" in
        -r | --recursive) recursive=1 ;;
        -ir | -ri)
            recursive=1
            args+=(-i)
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *) args+=("$arg") ;;
    esac
done

if [[ "$recursive" -eq 0 && "${#args[@]}" -eq 0 && -t 0 ]]; then
    echo "error: no input provided; refusing to wait on an interactive stdin" >&2
    usage >&2
    exit 64
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

if [[ "$recursive" -eq 0 ]]; then
    exec "$CLANG_FORMAT_BIN" "${args[@]+"${args[@]}"}"
fi

options=()
paths=()
for arg in "${args[@]+"${args[@]}"}"; do
    if [[ "$arg" == -* ]]; then
        options+=("$arg")
    else
        paths+=("$arg")
    fi
done
if [[ "${#paths[@]}" -eq 0 ]]; then
    paths=("$(git rev-parse --show-toplevel 2>/dev/null || pwd)")
fi
for path in "${paths[@]}"; do
    if [[ ! -e "$path" ]]; then
        echo "error: no such file or directory: $path" >&2
        exit 66
    fi
done

source_pattern='\.(c|cc|cpp|cxx|h|hh|hpp|hxx|ipp|inl)$'
list_sources() {
    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        # --full-name is not used: paths stay relative to the current
        # directory, which is where clang-format runs.
        git ls-files -z --cached --others --exclude-standard -- "${paths[@]}" |
            tr '\0' '\n' | grep -E "$source_pattern" || true
    else
        find "${paths[@]}" -type f | grep -E "$source_pattern" || true
    fi
}

mapfile -t files < <(list_sources | sort -u)
# Tracked files deleted in the work tree are still listed by git.
existing=()
for file in "${files[@]+"${files[@]}"}"; do
    [[ -f "$file" ]] && existing+=("$file")
done
if [[ "${#existing[@]}" -eq 0 ]]; then
    echo "clang-format.sh: no C/C++ sources under: ${paths[*]}" >&2
    exit 0
fi

printf '%s\0' "${existing[@]}" |
    xargs -0 "$CLANG_FORMAT_BIN" "${options[@]+"${options[@]}"}"
