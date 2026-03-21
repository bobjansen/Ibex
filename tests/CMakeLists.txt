include(Catch2Testing)

add_executable(ibex_tests
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
    test_websocket.cpp
)

target_include_directories(ibex_tests PRIVATE
    "${PROJECT_SOURCE_DIR}/libs/csv"
    "${PROJECT_SOURCE_DIR}/libs/json"
    "${PROJECT_SOURCE_DIR}/libs/websocket"
)

target_link_libraries(ibex_tests
    PRIVATE
        Ibex::core
        Ibex::ir
        Ibex::parser
        Ibex::repl
        Ibex::runtime
        Ibex::codegen
        Ibex::CompilerOptions
        Catch2::Catch2WithMain
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
if(IBEX_HAS_AVX2)
    target_compile_options(ibex_tests PRIVATE -mavx2 -mfma)
endif()

catch_discover_tests(ibex_tests)

if(IBEX_COVERAGE)
    ibex_add_coverage_target(ibex_tests)
endif()

if(TARGET ibex_eval_bin AND TARGET ibex_compile_bin)
    add_test(
        NAME ibex_parity_interpreter_vs_transpiled
        COMMAND "${CMAKE_COMMAND}" -E env
            "BUILD_DIR=${PROJECT_BINARY_DIR}"
            "CXX=${CMAKE_CXX_COMPILER}"
            "${PROJECT_SOURCE_DIR}/tests/parity/run_parity.sh"
    )
endif()
