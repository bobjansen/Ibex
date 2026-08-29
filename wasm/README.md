# Ibex WASM spike

Compiles the Ibex interpreter (parser + runtime + REPL session facade) to
WebAssembly so the browser UI can evaluate queries **with no local server** — a
static page on GitHub Pages, nothing to install.

Status: **spike / proof-of-concept.** It runs; it is not wired into `ui/` yet
and is not built in CI.

## What's here

| File | Purpose |
| --- | --- |
| `bridge.cpp` | embind glue. Exposes `execute(source) -> json` and `environment() -> json`, mirroring the shapes `src/ui/server.cpp` sends over HTTP. |
| `CMakeLists.txt` | The `ibex_wasm` target. Statically links the `data_gen` plugin (no `dlopen`) and embeds its `.ibex` stub into the module's virtual filesystem. |
| `demo/index.html` | Minimal standalone page — textarea, Run button, result table, timing. |
| `demo/smoke.mjs` | Node smoke test. |

## Building

Needs the Emscripten SDK (`emcc` on `PATH`). The root `CMakeLists.txt` detects
the toolchain and turns off everything host-specific (tools, tests, sockets,
heavyweight plugins).

```sh
emcmake cmake -S . -B build-wasm -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-wasm --target ibex_wasm
# -> build-wasm/wasm/ibex.mjs  +  build-wasm/wasm/ibex.wasm  (~4 MB, ~1.4 MB gzipped)
```

## Trying it

```sh
node wasm/demo/smoke.mjs            # headless

python3 -m http.server 8000        # then open
#   http://localhost:8000/wasm/demo/    (serve from the repo root)
```

## How it works / what was needed

- **No threads.** GitHub Pages can't send the COOP/COEP headers Emscripten
  pthreads require, so this builds single-threaded. `src/runtime/worker_pool.cpp`
  gained a `kInlinePool` path (compiled only for Emscripten-without-pthreads)
  that runs every `submit()` body inline on the caller. Native builds are
  untouched.
- **Plugins are static.** `data_gen`'s `ibex_register` is called directly and
  the library is marked registered, so `import data_gen;` resolves without
  `dlopen`. Its `.ibex` stub is embedded at `/ibex/data_gen.ibex`
  (`--embed-file`) and found via the session's import search path.
- **Two portability fixes** (both benign for native, verified against the full
  suite): an ADL ambiguity between `ibex::formatting::print` and libc++'s
  `std::print`, and a `sizeof(BuiltinFn)` guard that assumed a 64-bit pointer.

## Not done (next steps if this graduates)

- Wire into `ui/src/main.tsx` behind a build flag so the same UI runs against
  either the local server or the WASM module.
- Size pass (`-Oz`, `-sSTRICT`, closure) — the spike uses `-O2`.
- Bundle a few sample CSVs into MEMFS so `read_csv` works offline.
- CI job: build, run `smoke.mjs`, deploy `demo/` to Pages.
