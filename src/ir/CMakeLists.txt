add_library(ibex_ir STATIC
    node.cpp
    builder.cpp
)
add_library(Ibex::ir ALIAS ibex_ir)

target_include_directories(ibex_ir
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_ir
    PUBLIC
        Ibex::core
    PRIVATE
        Ibex::CompilerOptions
        fmt::fmt
        spdlog::spdlog
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_ir PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_ir
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
