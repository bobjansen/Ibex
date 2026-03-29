# CompilerOptions.cmake — Shared compiler configuration for Ibex targets

add_library(ibex_compiler_options INTERFACE)
add_library(Ibex::CompilerOptions ALIAS ibex_compiler_options)

target_compile_features(ibex_compiler_options INTERFACE cxx_std_23)

# Older Clang + libstdc++ combinations can report __cpp_concepts=201907L,
# which makes libstdc++ reject <expected>. Probe the active toolchain first
# and only enable the workaround when it is actually needed.
include(CheckCXXSourceCompiles)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
    set(_ibex_saved_required_flags "${CMAKE_REQUIRED_FLAGS}")
    string(APPEND CMAKE_REQUIRED_FLAGS " -std=c++23")
    check_cxx_source_compiles(
        "
        #include <expected>
        #ifndef __cpp_concepts
        #error __cpp_concepts missing
        #endif
        static_assert(__cpp_concepts >= 202002L);
        int main() {
            std::expected<int, int> x = 1;
            return *x;
        }
        "
        IBEX_TOOLCHAIN_HAS_WORKING_STD_EXPECTED
    )
    set(CMAKE_REQUIRED_FLAGS "${_ibex_saved_required_flags}")

    if(NOT IBEX_TOOLCHAIN_HAS_WORKING_STD_EXPECTED)
        message(STATUS "Ibex: enabling __cpp_concepts workaround for std::expected")
        target_compile_options(ibex_compiler_options
            INTERFACE
                -D__cpp_concepts=202002L
        )
    endif()
endif()

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
        $<$<CXX_COMPILER_ID:Clang,AppleClang>:-Wno-deprecated-builtins>
        $<$<CXX_COMPILER_ID:GNU>:-Wno-null-dereference>
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
