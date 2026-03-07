include(Catch2Testing)

add_executable(ibex_tests
    test_column.cpp
    test_csv.cpp
    test_json.cpp
    test_e2e.cpp
    test_ir_node.cpp
    test_lexer.cpp
    test_parser.cpp
    test_repl.cpp
    test_lower.cpp
    test_interpreter.cpp
    test_codegen.cpp
    test_join.cpp
)

target_include_directories(ibex_tests PRIVATE
    "${PROJECT_SOURCE_DIR}/libs/csv"
    "${PROJECT_SOURCE_DIR}/libs/json"
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

# Match ibex_runtime's AVX2 flags so that rng.hpp's #ifdef __AVX2__ guards
# agree between the test binary and the runtime library.  Without this,
# reseed_rng_x4() compiles to a no-op in the test while ibex_runtime still
# uses the x4 engine, breaking the reproducibility tests.
if(IBEX_HAS_AVX2)
    target_compile_options(ibex_tests PRIVATE -mavx2 -mfma)
endif()

catch_discover_tests(ibex_tests)
