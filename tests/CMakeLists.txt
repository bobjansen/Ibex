include(Catch2Testing)

find_package(SQLite3 REQUIRED)

add_executable(ibex_tests
    test_column.cpp
    test_csv.cpp
    test_sqlite.cpp
    test_ir_node.cpp
    test_parser.cpp
    test_repl.cpp
    test_lower.cpp
    test_interpreter.cpp
    test_codegen.cpp
    test_join.cpp
)

target_include_directories(ibex_tests
    PRIVATE
        "${PROJECT_SOURCE_DIR}/libs/csv"
        "${PROJECT_SOURCE_DIR}/libs/sqlite"
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
        SQLite::SQLite3
)

target_compile_definitions(ibex_tests
    PRIVATE
        IBEX_SOURCE_DIR="${PROJECT_SOURCE_DIR}"
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_tests PRIVATE Ibex::Sanitizers)
endif()

catch_discover_tests(ibex_tests)
