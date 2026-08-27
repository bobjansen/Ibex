#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

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
#        scripts/tidy-fix.sh <directories...>
#
# Requires build/compile_commands.json (run the normal cmake -B build config
# first). Only applies fixes for the checks above — this is not a general
# "run all lints" script; see .clang-tidy for the full check list.
#
# misc-const-correctness has a known false-positive pattern: it misses
# mutations that only happen inside a by-reference lambda capture, or inside
# an `if constexpr` branch. Both have been hit in this codebase (window.cpp,
# update.cpp) and are compile errors if the fix lands unreviewed. Every file
# is therefore recompiled (-fsyntax-only) after its fixes are applied, and a
# file whose fixes do not compile is restored and reported. That check is what
# makes this script safe to run unattended; it is not a substitute for
# rebuilding and running the tests, since a fix can compile and still be wrong.
#
# Unused-include REMOVAL is deliberately off (misc-include-cleaner only adds
# here). Removal is unsafe by construction in this codebase and the syntax
# check above cannot catch it: whether an include is used depends on which
# `#if` branches are live, and tidy sees exactly one configuration. Both
# branches of `IBEX_HAS_STD_CHRONO_TIME_ZONES` and every `_WIN32`/macOS
# fallback have includes that look unused from the other side — dropping one
# breaks a platform nobody builds here. Adding an include is safe in every
# configuration, which is the asymmetry this setting relies on.

set -euo pipefail

usage() {
    cat <<'EOF'
usage: scripts/tidy-fix.sh <files-or-directories...>

Examples:
  scripts/tidy-fix.sh src/runtime/chunked.cpp
  scripts/tidy-fix.sh src include
  git diff --name-only -- src include | grep -E '\.(cpp|hpp)$' | xargs scripts/tidy-fix.sh
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

# Header files do not have entries of their own in compile_commands.json, so
# clang-tidy's fallback command lacks the dependency include paths. Reuse every
# include path recorded by the configured project targets for standalone
# header processing (the paths are deduplicated by the helper).
header_include_args=()
while IFS= read -r arg; do
    header_include_args+=("--extra-arg=$arg")
done < <(python3 - "$BUILD_DIR/compile_commands.json" <<'PY'
import json
import shlex
import sys

with open(sys.argv[1]) as handle:
    entries = json.load(handle)

paths = set()
for entry in entries:
    argv = entry.get("arguments") or shlex.split(entry["command"])
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg.startswith("-I") and len(arg) > 2:
            paths.add(arg)
        elif arg == "-I" and index + 1 < len(argv):
            index += 1
            paths.add("-I" + argv[index])
        elif arg.startswith("-isystem") and len(arg) > len("-isystem"):
            paths.add("-isystem" + arg[len("-isystem"):])
        elif arg == "-isystem" and index + 1 < len(argv):
            index += 1
            paths.add("-isystem" + argv[index])
        index += 1

for path in sorted(paths):
    print(path)
PY
)

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

# clang-tidy accepts files, not directory operands. Expand directories here so
# the whole implementation and public-header trees can be tidied together.
files=()
for input in "$@"; do
    if [[ -d "$input" ]]; then
        while IFS= read -r -d '' file; do
            files+=("$file")
        done < <(find "$input" -type f \( -name '*.cpp' -o -name '*.hpp' \) -print0)
    else
        files+=("$input")
    fi
done
if [[ "${#files[@]}" -eq 0 ]]; then
    echo "error: no C++ files found in input directories" >&2
    exit 1
fi

# .clang-format intentionally leaves const placement alone (QualifierAlignment
# defaults to Leave) so it doesn't fight hand-written style elsewhere; add it
# just for this pass, in a scratch copy so the checked-in file is untouched.
STYLE_FILE="$(mktemp)"
# Pristine copies of every input, taken before any tool touches them, so the
# verification pass at the end can put back a file whose fixes do not compile.
# Keyed by index rather than path: the inputs may sit in different directories
# and share a basename.
ORIGINALS_DIR="$(mktemp -d)"
trap 'rm -rf -- "$STYLE_FILE" "$ORIGINALS_DIR"' EXIT
head -n -1 "$REPO_ROOT/.clang-format" > "$STYLE_FILE"  # drop trailing '...'
echo "QualifierAlignment: Left" >> "$STYLE_FILE"
echo "..." >> "$STYLE_FILE"

original_index=0
for f in "${files[@]}"; do
    cp -- "$f" "$ORIGINALS_DIR/$original_index"
    original_index=$((original_index + 1))
done

echo "Fixing includes, const-correctness, and designated initializers in: ${files[*]}"
# --header-filter='' restricts *fixes* (not just diagnostics) to the main
# file being processed — without it, clang-tidy will silently rewrite
# whatever project headers happen to get transitively included. Deliberately
# NOT --fix-errors: if the TU doesn't compile cleanly, bail out rather than
# apply fixes computed from a broken/error-recovery AST (verified this
# produces flatly wrong fixits, e.g. suggesting `const` on a variable
# mutated two lines later, once the AST was built past a #include error).
# Keep the checks that do not use ExprMutationAnalyzer together.  LLVM 23's
# misc-const-correctness can segfault while recursively analyzing a function's
# parameter mutations (currently triggered by interpreter.cpp).  Run it one
# file at a time so one compiler bug neither prevents the other fixes nor
# leaves us unable to identify the affected translation unit.
# `UnusedIncludes: false` — see the note at the top of this script. Spelled as
# --config rather than .clang-tidy so the repo-wide advisory CI sweep still
# *reports* unused includes; it is only automatic removal that is unsafe.
tidy_failed_files=()
for f in "${files[@]}"; do
    # A header often has no standalone compile command. clang-tidy can report
    # parser errors for it even though it compiles when included by a TU. Give
    # standalone headers the project's language mode; source files already get
    # it from their compile command. Keep any remaining failure local to the
    # file so it cannot block fixes elsewhere.
    tidy_extra_args=()
    if [[ "$f" == *.h || "$f" == *.hpp ]]; then
        tidy_extra_args+=(--extra-arg=-std=gnu++23)
        tidy_extra_args+=("${header_include_args[@]}")
    fi
    backup_file="$(mktemp)"
    cp -- "$f" "$backup_file"
    if "$CLANG_TIDY_BIN" -p "$BUILD_DIR" --header-filter='' \
        --config="{Checks: '-*,misc-include-cleaner,modernize-use-designated-initializers',
                   CheckOptions: {misc-include-cleaner.UnusedIncludes: false}}" \
        "${tidy_extra_args[@]}" \
        --fix "$f"; then
        :
    else
        tidy_failed_files+=("$f")
        cp -- "$backup_file" "$f"
    fi
    rm -f -- "$backup_file"
done

skipped_const_files=()
for f in "${files[@]}"; do
    backup_file="$(mktemp)"
    cp -- "$f" "$backup_file"
    tidy_extra_args=()
    if [[ "$f" == *.h || "$f" == *.hpp ]]; then
        tidy_extra_args+=(--extra-arg=-std=gnu++23)
        tidy_extra_args+=("${header_include_args[@]}")
    fi
    if "$CLANG_TIDY_BIN" -p "$BUILD_DIR" --header-filter='' \
        --checks='-*,misc-const-correctness' "${tidy_extra_args[@]}" --fix "$f"; then
        :
    else
        tidy_status=$?
        # A crashing tidy process may have written only a subset of its fixes.
        # Restore the pre-check version rather than leaving a half-applied edit.
        cp -- "$backup_file" "$f"
        if [[ "$tidy_status" -ge 128 ]]; then
            skipped_const_files+=("$f")
        else
            tidy_failed_files+=("$f")
        fi
    fi
    rm -f -- "$backup_file"
done

for f in "${files[@]}"; do
    # misc-include-cleaner's fixit inserts ibex/ headers quoted; the project
    # convention is angle brackets for every ibex/ header (see .clang-format's
    # IncludeCategories). Narrow to exactly that prefix so other quoted
    # same-directory includes (e.g. "interpreter_internal.hpp") are untouched.
    sed -i -E 's/#include "(ibex\/[^"]+)"/#include <\1>/' "$f"
    "$CLANG_FORMAT_BIN" -style="file:$STYLE_FILE" -i "$f"
done

# Verify each file still compiles, and put back the ones that do not. This is
# the guard against misc-const-correctness's known false positives, which are
# compile errors rather than subtle behaviour changes — a syntax-only rebuild
# of the affected TU catches all of them in about a second per file.
#
# It proves only that the tidy build's configuration still compiles. Anything
# configuration-dependent (a `#if`-guarded branch, a different platform) is
# outside its reach, which is why unused-include removal is disabled instead of
# being checked for.
reverted_files=()
verify_index=0
for f in "${files[@]}"; do
    original="$ORIGINALS_DIR/$verify_index"
    verify_index=$((verify_index + 1))
    if cmp -s -- "$f" "$original"; then
        continue  # untouched, and it compiled before we started
    fi
    echo "Verifying $f compiles..."
    if python3 - "$BUILD_DIR" "$f" <<'PY'
import json, os, shlex, subprocess, sys

build_dir, target = sys.argv[1], os.path.realpath(sys.argv[2])
with open(os.path.join(build_dir, "compile_commands.json")) as handle:
    entries = json.load(handle)

entry = next(
    (e for e in entries
     if os.path.realpath(os.path.join(e["directory"], e["file"])) == target),
    None,
)
header_fallback = entry is None and target.endswith((".h", ".hpp"))
if header_fallback:
    # Public headers do not appear in compile_commands.json. Use a configured
    # project command as the compiler context, replacing its TU with the
    # header and adding the union of the project's dependency include paths.
    entry = entries[0]
if entry is None:
    # clang-tidy needs a compile command too, so it cannot have fixed this
    # file. Say so rather than reporting a pass nobody verified.
    print(f"warning: no compile command for {target}; not verified", file=sys.stderr)
    sys.exit(0)

argv = entry.get("arguments") or shlex.split(entry["command"])
# Drop the object-file output and the -c that goes with it: -fsyntax-only
# writes nothing, and leaving them in earns an unused-argument warning on
# every file, which would bury the diagnostics this pass exists to surface.
command, skip = [], False
entry_source = os.path.realpath(os.path.join(entry["directory"], entry["file"]))
for arg in argv:
    if skip:
        skip = False
        continue
    if arg in ("-o", "-c"):
        skip = arg == "-o"
        continue
    if arg.startswith("-o") and len(arg) > 2:
        continue
    if os.path.realpath(arg) == entry_source:
        continue
    command.append(arg)
if header_fallback:
    include_args = set()
    for candidate in entries:
        candidate_argv = candidate.get("arguments") or shlex.split(candidate["command"])
        index = 0
        while index < len(candidate_argv):
            arg = candidate_argv[index]
            if arg.startswith("-I") and len(arg) > 2:
                include_args.add(arg)
            elif arg == "-I" and index + 1 < len(candidate_argv):
                index += 1
                include_args.add("-I" + candidate_argv[index])
            elif arg.startswith("-isystem") and len(arg) > len("-isystem"):
                include_args.add("-isystem" + arg[len("-isystem"):])
            elif arg == "-isystem" and index + 1 < len(candidate_argv):
                index += 1
                include_args.add("-isystem" + candidate_argv[index])
            index += 1
    command.extend(sorted(include_args))
    command.extend(["-std=gnu++23", "-x", "c++"])
command.append(target)
command.append("-fsyntax-only")
sys.exit(subprocess.call(command, cwd=entry["directory"]))
PY
    then
        :
    else
        cp -- "$original" "$f"
        reverted_files+=("$f")
    fi
done

if [[ "${#reverted_files[@]}" -ne 0 ]]; then
    echo >&2
    echo "error: fixes did not compile and were reverted for: ${reverted_files[*]}" >&2
    echo "       This is the misc-const-correctness false-positive pattern (a mutation" >&2
    echo "       inside a lambda capture or an uninstantiated 'if constexpr' branch)." >&2
    echo "       Other files kept their fixes; re-run after fixing these by hand." >&2
    exit 1
fi

if [[ "${#tidy_failed_files[@]}" -ne 0 ]]; then
    echo "warning: clang-tidy reported errors and skipped: ${tidy_failed_files[*]}" >&2
    echo "         Other files were still processed; fix the compile-command/setup issue and retry these." >&2
fi

echo "Done. Every changed file was recompiled and still builds. Review the diff,"
echo "then RUN THE TESTS before committing — compiling is not the same as correct."
if [[ "${#skipped_const_files[@]}" -ne 0 ]]; then
    echo "warning: misc-const-correctness crashed and was skipped for: ${skipped_const_files[*]}" >&2
    echo "         Safe tidy fixes were still applied; retry those files with a newer clang-tidy." >&2
fi
