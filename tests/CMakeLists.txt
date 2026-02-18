include(Catch2Testing)

add_executable(ibex_tests
    test_column.cpp
    test_ir_node.cpp
)

target_link_libraries(ibex_tests
    PRIVATE
        Ibex::core
        Ibex::ir
        Ibex::CompilerOptions
        Catch2::Catch2WithMain
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_tests PRIVATE Ibex::Sanitizers)
endif()

catch_discover_tests(ibex_tests)
