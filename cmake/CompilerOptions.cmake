# CompilerOptions.cmake — Shared compiler configuration for Ibex targets

add_library(ibex_compiler_options INTERFACE)
add_library(Ibex::CompilerOptions ALIAS ibex_compiler_options)

target_compile_features(ibex_compiler_options INTERFACE cxx_std_23)

target_compile_options(ibex_compiler_options
    INTERFACE
        $<$<CXX_COMPILER_ID:Clang,AppleClang,GNU>:
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
            -Wnon-virtual-dtor
            -Wold-style-cast
            -Wcast-align
            -Wunused
            -Woverloaded-virtual
            -Wnull-dereference
            -Wformat=2
        >
)

if(IBEX_WARNINGS_AS_ERRORS)
    target_compile_options(ibex_compiler_options
        INTERFACE
            $<$<CXX_COMPILER_ID:Clang,AppleClang,GNU>:-Werror>
    )
endif()

# Release optimizations
target_compile_options(ibex_compiler_options
    INTERFACE
        $<$<CONFIG:Release>:-O3 -DNDEBUG>
        $<$<CONFIG:Debug>:-O0 -g>
)

# Optional -march=native
if(IBEX_ENABLE_MARCH_NATIVE)
    target_compile_options(ibex_compiler_options
        INTERFACE
            $<$<CONFIG:Release>:-march=native>
    )
endif()

# Optional LTO
if(IBEX_ENABLE_LTO)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT lto_supported OUTPUT lto_error)
    if(lto_supported)
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
        message(STATUS "Ibex: LTO enabled for Release builds")
    else()
        message(WARNING "Ibex: LTO not supported — ${lto_error}")
    endif()
endif()
