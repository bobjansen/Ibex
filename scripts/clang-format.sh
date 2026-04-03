#!/usr/bin/env bash
# clang-format wrapper: choose the newest available clang-format.

set -euo pipefail

usage() {
    cat <<'EOF'
usage: scripts/clang-format.sh [clang-format args] <files...>

Examples:
  scripts/clang-format.sh -i src/core/foo.cpp
  git ls-files '*.cpp' '*.hpp' | xargs scripts/clang-format.sh -i
  echo 'int main(){return 0;}' | scripts/clang-format.sh
EOF
}

if [[ "$#" -eq 0 && -t 0 ]]; then
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

exec "$CLANG_FORMAT_BIN" "$@"
