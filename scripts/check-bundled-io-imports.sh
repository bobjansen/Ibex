#!/usr/bin/env bash
# Reject explicit bundled I/O extern declarations in canonical user-facing paths.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PATTERN='extern fn .* from "(csv|json|parquet)\.hpp";'
matches=""

check_file() {
    local path="$1"
    local found
    found="$(grep -nE "$PATTERN" "$path" || true)"
    if [[ -n "$found" ]]; then
        if [[ -n "$matches" ]]; then
            matches+=$'\n'
        fi
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            matches+="${path}:${line}"$'\n'
        done <<< "$found"
    fi
}

check_file "docs/index.html"
check_file "docs/io.html"
check_file "INSTALL.md"
while IFS= read -r path; do
    check_file "$path"
done < <(find examples -type f | sort)

matches="${matches%$'\n'}"
if [[ -n "$matches" ]]; then
    echo "error: bundled I/O examples must prefer import declarations in user-facing docs/examples" >&2
    echo >&2
    echo "$matches" >&2
    echo >&2
    echo 'Use `import "csv"`, `import "json"`, or `import "parquet"` instead.' >&2
    echo "Explicit bundled I/O extern declarations are reserved for stubs, tests, and plugin-internals coverage." >&2
    exit 1
fi

echo "bundled I/O import check passed"
