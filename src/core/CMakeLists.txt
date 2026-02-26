add_library(ibex_core STATIC
    column.cpp
)
add_library(Ibex::core ALIAS ibex_core)

target_include_directories(ibex_core
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_core
    PRIVATE
        Ibex::CompilerOptions
        fmt::fmt
        spdlog::spdlog
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_core PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_core
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
