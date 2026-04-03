include(Catch2Testing)

add_library(ibex_test_warning_suppressions INTERFACE)

set(IBEX_TEST_SOURCES
    catch_string_view_apple.cpp
    test_arrow_c_data.cpp
    test_column.cpp
    test_csv.cpp
    test_json.cpp
    test_e2e.cpp
    test_ir_node.cpp
    test_ir_optimizer.cpp
    test_lexer.cpp
    test_parser.cpp
    test_repl.cpp
    test_lower.cpp
    test_interpreter.cpp
    test_codegen.cpp
    test_join.cpp
)
if(NOT WIN32)
    list(APPEND IBEX_TEST_SOURCES test_websocket.cpp)
endif()

add_executable(ibex_tests ${IBEX_TEST_SOURCES})

target_include_directories(ibex_tests PRIVATE
    "${PROJECT_SOURCE_DIR}/libs/csv"
    "${PROJECT_SOURCE_DIR}/libs/json"
)
if(NOT WIN32)
    target_include_directories(ibex_tests PRIVATE
        "${PROJECT_SOURCE_DIR}/libs/websocket"
    )
endif()

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
        rapidcsv
        nlohmann_json::nlohmann_json
)

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
# a C2y extension under -Wpedantic. Keep the shared warnings for project code,
# but suppress this one test-framework-specific diagnostic after those flags.
target_compile_options(ibex_test_warning_suppressions
    INTERFACE
        $<$<CXX_COMPILER_ID:Clang>:-Wno-c2y-extensions>
        $<$<CXX_COMPILER_ID:AppleClang>:-Wno-c2x-extensions>
        $<$<CXX_COMPILER_ID:MSVC>:/wd4459>
)

catch_discover_tests(ibex_tests)

if(IBEX_COVERAGE)
    ibex_add_coverage_target(ibex_tests)
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
endif()
