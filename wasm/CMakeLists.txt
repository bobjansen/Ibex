# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# WASM spike: the Ibex interpreter compiled to WebAssembly + embind glue.
# Only reachable when configured with the Emscripten toolchain (see
# wasm/README.md); the root CMakeLists sets IBEX_WASM in that case.

if(NOT EMSCRIPTEN)
    message(FATAL_ERROR "wasm/ requires the Emscripten toolchain (emcmake cmake ...)")
endif()

set(IBEX_DATA_GEN_STUB "${PROJECT_SOURCE_DIR}/libs/data_gen/data_gen.ibex")

add_executable(ibex_wasm
    bridge.cpp
    "${PROJECT_SOURCE_DIR}/libs/data_gen/data_gen.cpp"
)

set_target_properties(ibex_wasm PROPERTIES OUTPUT_NAME ibex)

target_include_directories(ibex_wasm
    PRIVATE
        "${PROJECT_SOURCE_DIR}/include"
        "${PROJECT_SOURCE_DIR}/libs/data_gen"
)

target_link_libraries(ibex_wasm
    PRIVATE
        Ibex::repl
        Ibex::runtime
        Ibex::CompilerOptions
        nlohmann_json::nlohmann_json
)

# C++ exceptions are used throughout the interpreter and REPL; Emscripten
# disables catching by default.
target_compile_options(ibex_wasm PRIVATE -fexceptions -O2)

target_link_options(ibex_wasm PRIVATE
    -fexceptions
    -O2
    -lembind
    --embed-file "${IBEX_DATA_GEN_STUB}@/ibex/data_gen.ibex"
    -sMODULARIZE=1
    -sEXPORT_ES6=1
    -sEXPORT_NAME=createIbex
    -sENVIRONMENT=web,worker,node
    -sALLOW_MEMORY_GROWTH=1
    -sINITIAL_MEMORY=64MB
    -sSTACK_SIZE=4MB
    -sEXIT_RUNTIME=0
    -sFILESYSTEM=1
)

# `.mjs` so the ES6 module loads directly in a browser <script type=module> and
# under Node without a package.json "type" override.
set_target_properties(ibex_wasm PROPERTIES SUFFIX ".mjs")
