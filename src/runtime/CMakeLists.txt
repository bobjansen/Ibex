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

# ── AVX2 / libmvec ────────────────────────────────────────────────────────────
# Enable when the compiler supports -mavx2.  The #ifdef __AVX2__ guards in
# rng.hpp make this a no-op on platforms that lack AVX2.
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-mavx2 IBEX_HAS_AVX2)
if(IBEX_HAS_AVX2)
    target_compile_options(ibex_runtime PRIVATE -mavx2 -mfma)
    # libmvec (glibc ≥ 2.22) provides 4-wide double transcendentals via the
    # GNU Vector SIMD ABI (_ZGVdN4v_log/cos/sin).  Define IBEX_HAVE_LIBMVEC so
    # rng.hpp can gate those declarations — without this guard, non-glibc builds
    # (musl, macOS) would fail to link even though they have AVX2.
    find_library(IBEX_LIBMVEC mvec)
    if(IBEX_LIBMVEC)
        target_link_libraries(ibex_runtime PRIVATE "${IBEX_LIBMVEC}")
        target_compile_definitions(ibex_runtime PRIVATE IBEX_HAVE_LIBMVEC=1)
    endif()
endif()

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_runtime PRIVATE Ibex::Sanitizers)
endif()

install(TARGETS ibex_runtime
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)
