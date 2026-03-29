add_library(ibex_interop STATIC
    arrow_c_data.cpp
)
add_library(Ibex::interop ALIAS ibex_interop)

target_include_directories(ibex_interop
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_interop
    PUBLIC
        Ibex::runtime
    PRIVATE
        Ibex::CompilerOptions
        fmt::fmt
        spdlog::spdlog
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_interop PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_interop
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
