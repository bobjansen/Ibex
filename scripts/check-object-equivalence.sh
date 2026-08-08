#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Compare a release-build object before and after a small source change.
#
# Usage:
#   scripts/check-object-equivalence.sh [-B <build-dir>] snapshot <object>
#   scripts/check-object-equivalence.sh [-B <build-dir>] compare <target> <object>
#
# `object` may be relative to the build directory or absolute.  Snapshot it
# before editing, then compare after editing; compare rebuilds <target> first.
#
# Example:
#   scripts/check-object-equivalence.sh snapshot src/core/CMakeFiles/ibex_core.dir/foo.cpp.o
#   # make a lint-only edit
#   scripts/check-object-equivalence.sh compare ibex_core src/core/CMakeFiles/ibex_core.dir/foo.cpp.o

set -euo pipefail

usage() {
    sed -n '2,15p' "$0" >&2
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ibex_root="${IBEX_ROOT:-$(dirname "$script_dir")}"
build_dir="${IBEX_BUILD_DIR:-$ibex_root/build-release}"

while getopts ":B:h" option; do
    case "$option" in
        B) build_dir="$OPTARG" ;;
        h) usage; exit 0 ;;
        *) usage; exit 64 ;;
    esac
done
shift $((OPTIND - 1))

if [[ "$#" -lt 2 ]]; then
    usage
    exit 64
fi

mode="$1"
shift

case "$mode" in
    snapshot)
        if [[ "$#" -ne 1 ]]; then
            usage
            exit 64
        fi
        target=""
        object_arg="$1"
        ;;
    compare)
        if [[ "$#" -ne 2 ]]; then
            usage
            exit 64
        fi
        target="$1"
        object_arg="$2"
        ;;
    *)
        echo "error: expected 'snapshot' or 'compare', got '$mode'" >&2
        usage
        exit 64
        ;;
esac

if [[ "$object_arg" = /* ]]; then
    object="$object_arg"
else
    object="$build_dir/$object_arg"
fi

if [[ ! -f "$object" ]]; then
    echo "error: object does not exist: $object" >&2
    exit 1
fi

# Keep baselines outside the checkout and identify them by their complete path.
baseline_dir="${TMPDIR:-/tmp}/ibex-object-equivalence"
mkdir -p "$baseline_dir"
object_key="$(printf '%s' "$object" | sha256sum | cut -d' ' -f1)"
baseline="$baseline_dir/$object_key.before.o"

if [[ "$mode" == "snapshot" ]]; then
    cp "$object" "$baseline"
    echo "▸ saved baseline: $baseline"
    exit 0
fi

if [[ ! -f "$baseline" ]]; then
    echo "error: no baseline for $object; run the snapshot command before editing" >&2
    exit 1
fi

echo "▸ rebuilding $target"
cmake --build "$build_dir" --parallel --target "$target"

if cmp -s "$baseline" "$object"; then
    echo "✓ identical object file: $object"
    exit 0
fi

echo "✗ object changed: $object" >&2
echo "  baseline retained at: $baseline" >&2
exit 1
