add_library(ibex_runtime STATIC
    extern_registry.cpp
    interpreter.cpp
    ops.cpp
)
add_library(Ibex::runtime ALIAS ibex_runtime)

target_include_directories(ibex_runtime
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_runtime
    PUBLIC
        Ibex::core
    PRIVATE
        Ibex::CompilerOptions
        fmt::fmt
        spdlog::spdlog
        robin_hood::robin_hood
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_runtime PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_runtime
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
