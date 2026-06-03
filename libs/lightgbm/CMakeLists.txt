include(FetchContent)

# --- Real LightGBM (Microsoft) via FetchContent -----------------------------
# Build a lean static library and link its C API into the plugin. OpenMP is
# disabled for now: Ibex is single-core by design and this avoids a hard
# dependency on libomp for the clang toolchain. The CLI/R/Python artifacts are
# not needed.
set(USE_OPENMP OFF CACHE BOOL "" FORCE)
set(BUILD_CLI OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC_LIB ON CACHE BOOL "" FORCE)
set(__BUILD_FOR_R OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    LightGBM
    GIT_REPOSITORY https://github.com/microsoft/LightGBM.git
    GIT_TAG        v4.5.0
    GIT_SHALLOW    TRUE
)
# LightGBM emits noisy developer warnings; quiet them like the Arrow plugin does.
set(_ibex_saved_suppress_dev_warnings "${CMAKE_SUPPRESS_DEVELOPER_WARNINGS}")
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1)
FetchContent_MakeAvailable(LightGBM)
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS "${_ibex_saved_suppress_dev_warnings}")

# Resolve the LightGBM library target across versions (static preferred).
if(TARGET lightgbm_static)
    set(_ibex_lightgbm_lib lightgbm_static)
elseif(TARGET _lightgbm)
    set(_ibex_lightgbm_lib _lightgbm)
elseif(TARGET lib_lightgbm)
    set(_ibex_lightgbm_lib lib_lightgbm)
else()
    message(FATAL_ERROR "Could not find a LightGBM library target after FetchContent.")
endif()

add_library(ibex_lightgbm_plugin SHARED lightgbm.cpp)

set_target_properties(ibex_lightgbm_plugin PROPERTIES
    OUTPUT_NAME lightgbm
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_lightgbm_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
)

target_include_directories(ibex_lightgbm_plugin
    SYSTEM PRIVATE
        "${lightgbm_SOURCE_DIR}/include"
)

target_link_libraries(ibex_lightgbm_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
        ${_ibex_lightgbm_lib}
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_lightgbm_plugin PRIVATE Ibex::Sanitizers)
endif()

add_custom_command(TARGET ibex_lightgbm_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/lightgbm.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/lightgbm.ibex"
    COMMENT "Installing lightgbm.ibex stub"
)
