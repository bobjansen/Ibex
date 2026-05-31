find_package(CURL REQUIRED)

add_executable(ibex_repl_bin ibex_repl.cpp)

set_target_properties(ibex_repl_bin PROPERTIES
    OUTPUT_NAME ibex
)

target_link_libraries(ibex_repl_bin
    PRIVATE
        Ibex::repl
        Ibex::CompilerOptions
        CLI11::CLI11
        fmt::fmt
        spdlog::spdlog
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_repl_bin PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_repl_bin
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

add_executable(ibex_bench ibex_bench.cpp)

target_include_directories(ibex_bench PRIVATE "${PROJECT_SOURCE_DIR}/libs/csv")

target_link_libraries(ibex_bench
    PRIVATE
        Ibex::parser
        Ibex::runtime
        Ibex::CompilerOptions
        CLI11::CLI11
        fmt::fmt
        fast_float
        CURL::libcurl
)

# zorro.hpp (included via rng.hpp) uses compile-time SIMD dispatch.
# Mirror the same AVX2 flag used by ibex_runtime so the benchmark
# measures what actually runs in production.
if(IBEX_TARGETS_X86 AND IBEX_HAS_AVX2)
    if(MSVC)
        target_compile_options(ibex_bench PRIVATE /arch:AVX2)
    else()
        target_compile_options(ibex_bench PRIVATE -mavx2 -mfma)
    endif()
endif()

# Link jemalloc when available.  jemalloc pools large freed allocations instead of
# returning them to the OS via munmap.  Without it, glibc mmap-allocates every
# std::vector > 128 KB and immediately munmaps on free, so every benchmark
# iteration pays minor page-fault overhead proportional to its output size
# (≈ 100-150 ms per melt run at 4 M rows).  With jemalloc the warm iterations
# reuse pooled memory — page faults only on the first two warm-up runs.
find_library(JEMALLOC_LIB
    NAMES jemalloc libjemalloc.so.2
    HINTS /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu
)
if(JEMALLOC_LIB)
    target_link_libraries(ibex_bench PRIVATE ${JEMALLOC_LIB})
    message(STATUS "ibex_bench: linking jemalloc (${JEMALLOC_LIB})")
else()
    # Loud in the build log so a benchmark box silently falling back to glibc
    # malloc (and its 32MB mmap-threshold page-fault cliff above 4M rows) is
    # caught — install libjemalloc-dev. The runtime's mallopt() fallback
    # mitigates glibc, but jemalloc is the intended config for credible numbers.
    message(WARNING "ibex_bench: jemalloc NOT found — using glibc malloc; "
                    "benchmark numbers above ~4M rows will show a page-fault cliff. "
                    "Install libjemalloc-dev for the intended allocator.")
endif()

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_bench PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_bench
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

add_executable(ibex_join_bench ibex_join_bench.cpp)

target_link_libraries(ibex_join_bench
    PRIVATE
        Ibex::parser
        Ibex::runtime
        Ibex::CompilerOptions
        CLI11::CLI11
        fmt::fmt
)

if(IBEX_TARGETS_X86 AND IBEX_HAS_AVX2)
    if(MSVC)
        target_compile_options(ibex_join_bench PRIVATE /arch:AVX2)
    else()
        target_compile_options(ibex_join_bench PRIVATE -mavx2 -mfma)
    endif()
endif()

find_library(JEMALLOC_LIB_JOIN
    NAMES jemalloc libjemalloc.so.2
    HINTS /usr/lib/x86_64-linux-gnu /lib/x86_64-linux-gnu
)
if(JEMALLOC_LIB_JOIN)
    target_link_libraries(ibex_join_bench PRIVATE ${JEMALLOC_LIB_JOIN})
endif()

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_join_bench PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_join_bench
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

add_executable(ibex_fusion_bench ibex_fusion_bench.cpp)

target_link_libraries(ibex_fusion_bench
    PRIVATE
        Ibex::parser
        Ibex::runtime
        Ibex::CompilerOptions
        CLI11::CLI11
        fmt::fmt
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_fusion_bench PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_fusion_bench
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

add_executable(ibex_bench_csv_read bench_read_csv.cpp)

target_include_directories(ibex_bench_csv_read PRIVATE "${PROJECT_SOURCE_DIR}/libs/csv")

target_link_libraries(ibex_bench_csv_read
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
        CLI11::CLI11
        fast_float
        CURL::libcurl
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_bench_csv_read PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_bench_csv_read
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

add_library(ibex_gen_tf_data_plugin SHARED gen_tf_data.cpp)

set_target_properties(ibex_gen_tf_data_plugin PROPERTIES
    OUTPUT_NAME gen_tf_data
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_gen_tf_data_plugin
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}"
)

target_link_libraries(ibex_gen_tf_data_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_gen_tf_data_plugin PRIVATE Ibex::Sanitizers)
endif()

add_executable(ibex_compile_bin ibex_compile.cpp)

set_target_properties(ibex_compile_bin PROPERTIES
    OUTPUT_NAME ibex_compile
)

target_link_libraries(ibex_compile_bin
    PRIVATE
        Ibex::codegen
        Ibex::parser
        Ibex::CompilerOptions
        CLI11::CLI11
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_compile_bin PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_compile_bin
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)

add_executable(ibex_eval_bin ibex_eval.cpp)

set_target_properties(ibex_eval_bin PROPERTIES
    OUTPUT_NAME ibex_eval
)

target_link_libraries(ibex_eval_bin
    PRIVATE
        Ibex::repl
        Ibex::parser
        Ibex::runtime
        Ibex::CompilerOptions
        CLI11::CLI11
        spdlog::spdlog
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_eval_bin PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_eval_bin
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)
