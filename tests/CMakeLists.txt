include(Catch2Testing)

add_executable(ibex_tests
    test_column.cpp
    test_csv.cpp
    test_ir_node.cpp
    test_parser.cpp
    test_repl.cpp
    test_lower.cpp
    test_interpreter.cpp
)

target_link_libraries(ibex_tests
    PRIVATE
        Ibex::core
        Ibex::ir
        Ibex::parser
        Ibex::repl
        Ibex::runtime
        Ibex::CompilerOptions
        Catch2::Catch2WithMain
)

target_compile_definitions(ibex_tests
    PRIVATE
        IBEX_SOURCE_DIR="${PROJECT_SOURCE_DIR}"
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_tests PRIVATE Ibex::Sanitizers)
endif()

catch_discover_tests(ibex_tests)
