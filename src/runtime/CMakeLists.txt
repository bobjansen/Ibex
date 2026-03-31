add_library(ibex_runtime STATIC
    extern_registry.cpp
    interpreter.cpp
    join.cpp
    model.cpp
    ops.cpp
    reshape.cpp
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

# ── AVX2 / libmvec ────────────────────────────────────────────────────────────
# Enable when the compiler supports -mavx2.  zorro.hpp (included via rng.hpp)
# uses compile-time SIMD dispatch: __AVX512F__ → __AVX2__ → portable.
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-mavx2 IBEX_HAS_AVX2)
if(IBEX_HAS_AVX2)
    target_compile_options(ibex_runtime PRIVATE -mavx2 -mfma)
    # libmvec (glibc ≥ 2.22) provides vectorized transcendentals.
    # ZORRO_USE_LIBMVEC enables zorro's AVX2/512 log/cos/sin kernels.
    find_library(IBEX_LIBMVEC mvec)
    if(IBEX_LIBMVEC)
        target_link_libraries(ibex_runtime PRIVATE "${IBEX_LIBMVEC}")
        target_compile_definitions(ibex_runtime PRIVATE ZORRO_USE_LIBMVEC=1)
    endif()
endif()

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_runtime PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_runtime
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
