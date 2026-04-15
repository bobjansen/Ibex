#!/usr/bin/env bash
# Reject explicit bundled I/O extern declarations in canonical user-facing paths.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PATTERN='extern fn .* from "(csv|json|parquet)\.hpp";'
TARGETS=(
    docs/index.html
    docs/io.html
    INSTALL.md
    examples
)

if ! command -v rg >/dev/null 2>&1; then
    echo "error: ripgrep (rg) is required for scripts/check-bundled-io-imports.sh" >&2
    exit 2
fi

matches="$(rg -n "$PATTERN" "${TARGETS[@]}" || true)"
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
