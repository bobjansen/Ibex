#!/usr/bin/env bash
# run_demo.sh — full Ibex Brownian motion streaming demo.
#
# What this script does:
#   1. Builds tick_gen and monitor (C++23, -O2, no external deps)
#   2. Transpiles brownian_stream.ibex → C++ via ibex_compile
#   3. Compiles the generated C++ against the Ibex release libraries
#   4. Starts all three processes, wires them together, cleans up on Ctrl+C
#
# Data flow:
#   tick_gen  →  UDP:9001  →  ibex binary  →  WS:8765  →  monitor
#
# Env overrides:
#   IBEX_ROOT  — repo root          (default: two dirs above this script)
#   BUILD_DIR  — cmake build dir    (default: $IBEX_ROOT/build-release)
#   CXX        — C++ compiler       (default: clang++)
#   TICK_RATE  — ticks/sec total    (default: 20)
#   SIGMA      — per-tick GBM vol   (default: 0.0015)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build-release}"
CXX="${CXX:-clang++}"
TICK_RATE="${TICK_RATE:-20}"
SIGMA="${SIGMA:-0.0015}"
UDP_PORT=9001
WS_PORT=8765

IBEX_COMPILE="$BUILD_DIR/tools/ibex_compile"
IBEX_FILE="$SCRIPT_DIR/brownian_stream.ibex"

# ── Dependency checks ─────────────────────────────────────────────────────────

if [[ ! -x "$IBEX_COMPILE" ]]; then
    echo "error: ibex_compile not found at $IBEX_COMPILE"
    echo "       Run: cmake --build $BUILD_DIR --target ibex_compile_bin"
    exit 1
fi

# ── Include / library paths (mirrors ibex-run.sh) ────────────────────────────

IBEX_INCS=(
    "-I$IBEX_ROOT/include"
    "-I$IBEX_ROOT/libraries"
    "-I$BUILD_DIR/_deps/fmt-src/include"
    "-I$BUILD_DIR/_deps/spdlog-src/include"
    "-I$BUILD_DIR/_deps/robin_hood-src/src/include"
)
if [[ -d "$IBEX_ROOT/libs" ]]; then
    while IFS= read -r -d '' lib_dir; do
        IBEX_INCS+=("-I$lib_dir")
    done < <(find "$IBEX_ROOT/libs" -mindepth 1 -maxdepth 1 -type d -print0)
fi

_fmt_lib="$BUILD_DIR/_deps/fmt-build/libfmt.a"
[[ -f "$_fmt_lib" ]] || _fmt_lib="$BUILD_DIR/_deps/fmt-build/libfmtd.a"
_spdlog_lib="$BUILD_DIR/_deps/spdlog-build/libspdlog.a"
[[ -f "$_spdlog_lib" ]] || _spdlog_lib="$BUILD_DIR/_deps/spdlog-build/libspdlogd.a"

IBEX_LIBS=(
    "$BUILD_DIR/src/runtime/libibex_runtime.a"
    "$BUILD_DIR/src/ir/libibex_ir.a"
    "$BUILD_DIR/src/core/libibex_core.a"
    "$_fmt_lib"
    "$_spdlog_lib"
)

# ── Build directory for compiled artefacts ────────────────────────────────────

BIN_DIR="$SCRIPT_DIR/build"
mkdir -p "$BIN_DIR"

TICK_GEN="$BIN_DIR/tick_gen"
MONITOR="$BIN_DIR/monitor"
STREAM_CPP="$BIN_DIR/brownian_stream.cpp"
STREAM_BIN="$BIN_DIR/brownian_stream"

# ── Cleanup: kill all background processes on exit ───────────────────────────

PIDS=()
cleanup() {
    echo ""
    echo "▸ shutting down ..."
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    # Give processes a moment to exit cleanly before the script ends.
    sleep 0.3
}
trap cleanup EXIT INT TERM

# ── Step 1: compile tick_gen and monitor ─────────────────────────────────────

echo "▸ compiling  tick_gen.cpp"
"$CXX" -std=c++23 -O2 -o "$TICK_GEN" "$SCRIPT_DIR/tick_gen.cpp"

echo "▸ compiling  monitor.cpp"
"$CXX" -std=c++23 -O2 -o "$MONITOR" "$SCRIPT_DIR/monitor.cpp"

# ── Step 2: transpile the Ibex script ────────────────────────────────────────

echo "▸ transpiling brownian_stream.ibex"
"$IBEX_COMPILE" "$IBEX_FILE" -o "$STREAM_CPP"

# ── Step 3: compile the generated C++ ────────────────────────────────────────

echo "▸ compiling  brownian_stream.cpp"
"$CXX" -std=c++23 -O2 "${IBEX_INCS[@]}" "$STREAM_CPP" "${IBEX_LIBS[@]}" -o "$STREAM_BIN"

# ── Step 4: start the Ibex stream binary ─────────────────────────────────────

echo ""
echo "▸ starting   ibex stream   (UDP:$UDP_PORT → resample 10s by symbol → WS:$WS_PORT)"
"$STREAM_BIN" &
PIDS+=($!)

# ── Step 5: wait for the WebSocket port to become available ──────────────────

echo -n "▸ waiting for WS:$WS_PORT "
for _ in $(seq 1 50); do
    if python3 -c "
import socket, sys
try:
    s = socket.create_connection(('127.0.0.1', $WS_PORT), 0.1)
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
        echo " ready"
        break
    fi
    echo -n "."
    sleep 0.1
done

# ── Step 6: start the monitor ────────────────────────────────────────────────

echo "▸ starting   monitor       (WS:$WS_PORT → terminal)"
"$MONITOR" --port "$WS_PORT" &
PIDS+=($!)
sleep 0.3  # let the monitor connect before ticks start

# ── Step 7: run the tick generator ───────────────────────────────────────────

echo "▸ starting   tick_gen      (GBM × 4 symbols → UDP:$UDP_PORT)"
echo ""
echo "  Ctrl+C to stop all processes."
echo ""
"$TICK_GEN" --rate "$TICK_RATE" --sigma "$SIGMA"
