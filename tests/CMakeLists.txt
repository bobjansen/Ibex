# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

include(Catch2Testing)

add_library(ibex_test_warning_suppressions INTERFACE)

set(IBEX_TEST_SOURCES
    catch_string_view_apple.cpp
    test_adbc_options.cpp
    test_arrow_c_data.cpp
    test_args.cpp
    test_column.cpp
    test_csv.cpp
    test_decimal.cpp
    test_decimal_e2e.cpp
    test_fs.cpp
    test_json.cpp
    test_e2e.cpp
    test_e2e_lazy_queries.cpp
    test_execution_profile.cpp
    test_gather_kernel.cpp
    test_ir_canonicalize.cpp
    test_ir_cardinality.cpp
    test_ir_extern_sources.cpp
    test_ir_expr_predicates.cpp
    test_ir_join_pushdown.cpp
    test_ir_count_distinct_reduction.cpp
    test_ir_join_semi_reduction.cpp
    test_ir_mapped_join_keys.cpp
    test_ir_join_order.cpp
    test_ir_join_output.cpp
    test_ir_join_reorder.cpp
    test_kernel_types.cpp
    test_physical_plan.cpp
    test_ir_node.cpp
    test_ir_optimizer.cpp
    test_ir_pending_order.cpp
    test_ir_column_origins.cpp
    test_parquet_dictionary_policy.cpp
    test_ir_distinct_key_reduction.cpp
    test_ir_group_key_reduction.cpp
    test_ir_required_columns.cpp
    test_ir_schema.cpp
    test_kafka_common.cpp
    test_kafka_schema_registry.cpp
    test_kafka_schema_registry_client.cpp
    test_lexer.cpp
    test_parser.cpp
    test_repl.cpp
    test_lower.cpp
    test_null_keys.cpp
    test_interpreter.cpp
    test_interrupt.cpp
    test_codegen.cpp
    test_filter_range.cpp
    test_table_properties.cpp
    test_filter_selection.cpp
    test_join.cpp
    test_lazy_table.cpp
    test_chunked_sources.cpp
    test_operator.cpp
    test_table_compare.cpp
    test_worker_pool.cpp
)
if(NOT WIN32)
    list(APPEND IBEX_TEST_SOURCES test_websocket.cpp test_udp.cpp)
endif()
if(TARGET avrocpp_s OR TARGET avrocpp)
    list(APPEND IBEX_TEST_SOURCES test_kafka_avro.cpp)
endif()

add_executable(ibex_tests ${IBEX_TEST_SOURCES})

target_include_directories(ibex_tests PRIVATE
    # test_filter_range.cpp exercises the range-aware evaluators, which are
    # declared in the runtime's internal headers rather than the public API.
    "${PROJECT_SOURCE_DIR}/src/runtime"
    # adbc_options.hpp is header-only and has no ADBC dependency.
    "${PROJECT_SOURCE_DIR}/libs/adbc"
    "${PROJECT_SOURCE_DIR}/libs/args"
    "${PROJECT_SOURCE_DIR}/libs/csv"
    "${PROJECT_SOURCE_DIR}/libs/fs"
    "${PROJECT_SOURCE_DIR}/libs/kafka"
    "${PROJECT_SOURCE_DIR}/libs/json"
)
if(NOT WIN32)
    target_include_directories(ibex_tests PRIVATE
        "${PROJECT_SOURCE_DIR}/libs/websocket"
        "${PROJECT_SOURCE_DIR}/libs/udp"
    )
endif()

# CURL is optional (see libs/csv/CMakeLists.txt): test_csv.cpp only needs it
# to exercise read_csv's HTTPS path, which is itself optional.
find_package(CURL)

target_link_libraries(ibex_tests
    PRIVATE
        Ibex::core
        Ibex::ir
        Ibex::parser
        Ibex::repl
        Ibex::runtime
        Ibex::interop
        Ibex::codegen
        Ibex::CompilerOptions
        Catch2::Catch2WithMain
        ibex_test_warning_suppressions
        nlohmann_json::nlohmann_json
        fast_float
)

if(CURL_FOUND)
    target_link_libraries(ibex_tests PRIVATE CURL::libcurl)
    target_compile_definitions(ibex_tests PRIVATE IBEX_CSV_HAVE_CURL)
endif()
if(TARGET avrocpp_s)
    target_link_libraries(ibex_tests PRIVATE avrocpp_s)
elseif(TARGET avrocpp)
    target_link_libraries(ibex_tests PRIVATE avrocpp)
endif()

target_compile_definitions(ibex_tests
    PRIVATE
        IBEX_SOURCE_DIR="${PROJECT_SOURCE_DIR}"
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_tests PRIVATE Ibex::Sanitizers)
endif()

# Match ibex_runtime's AVX2 flags so that zorro.hpp's SIMD dispatch agrees
# between the test binary and the runtime library.
if(IBEX_TARGETS_X86 AND IBEX_HAS_AVX2 AND NOT MSVC)
    target_compile_options(ibex_tests PRIVATE -mavx2 -mfma)
endif()

# Catch2 uses __COUNTER__ heavily in its test macros, which Clang diagnoses as
# a C extension under -Wpedantic. Keep the shared warnings for project code,
# but suppress this one test-framework-specific diagnostic after those flags.
# The warning's spelling differs by Clang version — `c2x` on Clang <= 18,
# `c2y` on newer — so pass both and tolerate whichever the compiler does not
# recognize via -Wno-unknown-warning-option (otherwise the unknown flag is
# fatal under -Werror, which broke the clang-18 release build).
target_compile_options(ibex_test_warning_suppressions
    INTERFACE
        $<$<CXX_COMPILER_ID:Clang,AppleClang>:-Wno-unknown-warning-option>
        $<$<CXX_COMPILER_ID:Clang,AppleClang>:-Wno-c2y-extensions>
        $<$<CXX_COMPILER_ID:Clang,AppleClang>:-Wno-c2x-extensions>
        $<$<CXX_COMPILER_ID:MSVC>:/wd4459>
)

if(WIN32)
    # Use PRE_TEST discovery on Windows to avoid codepage issues with
    # UTF-8 test names (em-dashes, arrows) during POST_BUILD discovery.
    catch_discover_tests(ibex_tests DISCOVERY_MODE PRE_TEST)
else()
    catch_discover_tests(ibex_tests)
endif()

if(IBEX_COVERAGE)
    ibex_add_coverage_target(ibex_tests)
endif()

# ADBC integration tests: load the built adbc plugin through `import "adbc"` and
# read from a throwaway SQLite database. A separate executable so it exists only
# when the plugin and a SQLite driver do. It deliberately does NOT link the
# driver manager: that would put the driver's environment (e.g. a conda lib/
# with its own libreadline) on this binary's RPATH, ahead of system libraries.
if(TARGET ibex_adbc_plugin AND ADBC_DRIVER_SQLITE_LIBRARY)
    add_executable(ibex_adbc_tests test_adbc.cpp)
    add_dependencies(ibex_adbc_tests ibex_adbc_plugin)
    target_link_libraries(ibex_adbc_tests
        PRIVATE
            Ibex::core
            Ibex::runtime
            Ibex::repl
            Ibex::CompilerOptions
            Catch2::Catch2WithMain
            ibex_test_warning_suppressions
    )
    target_compile_definitions(ibex_adbc_tests
        PRIVATE
            IBEX_ADBC_PLUGIN_DIR="${IBEX_PLUGIN_OUTPUT_DIR}"
            IBEX_ADBC_SQLITE_DRIVER="${ADBC_DRIVER_SQLITE_LIBRARY}"
    )
    if(TARGET Ibex::Sanitizers)
        target_link_libraries(ibex_adbc_tests PRIVATE Ibex::Sanitizers)
    endif()
    if(IBEX_TARGETS_X86 AND IBEX_HAS_AVX2 AND NOT MSVC)
        target_compile_options(ibex_adbc_tests PRIVATE -mavx2 -mfma)
    endif()
    # Prefixed so `ctest -R '^adbc:'` selects exactly these, without also
    # matching ibex_tests cases such as the options-grammar tests. (No space
    # after the colon: discovery drops it.)
    catch_discover_tests(ibex_adbc_tests TEST_PREFIX "adbc:")

    # The examples/adbc_sqlite walkthrough, run end to end through the ibex CLI
    # and checked against its expected summary, so the demo cannot rot.
    if(TARGET ibex_repl_bin AND TARGET ibex_args_plugin AND TARGET ibex_csv_plugin AND NOT WIN32)
        add_test(NAME adbc:sqlite_demo
            COMMAND bash "${PROJECT_SOURCE_DIR}/examples/adbc_sqlite/run.sh")
        set_tests_properties(adbc:sqlite_demo PROPERTIES
            ENVIRONMENT "IBEX_BIN=$<TARGET_FILE:ibex_repl_bin>;IBEX_PLUGIN_DIR=${IBEX_PLUGIN_OUTPUT_DIR};ADBC_DRIVER_SQLITE=${ADBC_DRIVER_SQLITE_LIBRARY}")
    endif()
endif()

if(TARGET ibex_eval_bin AND TARGET ibex_compile_bin AND NOT WIN32)
    set(_parity_cxxflags "")
    set(_parity_ldflags "")
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(_parity_cxxflags "-fsanitize=address,undefined -fno-omit-frame-pointer")
        set(_parity_ldflags "-fsanitize=address,undefined")
    endif()
    add_test(
        NAME ibex_parity_interpreter_vs_transpiled
        COMMAND "${CMAKE_COMMAND}" -E env
            "BUILD_DIR=${PROJECT_BINARY_DIR}"
            "CXX=${CMAKE_CXX_COMPILER}"
            "PARITY_CXXFLAGS=${_parity_cxxflags}"
            "PARITY_LDFLAGS=${_parity_ldflags}"
            "${PROJECT_SOURCE_DIR}/tests/parity/run_parity.sh"
    )
    # This test builds and runs both the interpreter and every transpiled case
    # from scratch (~5 min), unlike the Catch2 tests it runs alongside, which
    # finish in seconds. Label it so the fast inner loop can skip it
    # (`ctest -LE slow`) while CI and pre-push runs still include it via a
    # plain `ctest`.
    set_tests_properties(ibex_parity_interpreter_vs_transpiled PROPERTIES LABELS "slow")
endif()
