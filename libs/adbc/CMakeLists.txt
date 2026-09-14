# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

add_library(ibex_adbc_plugin SHARED adbc.cpp)

# The driver manager (the library that loads ADBC drivers) is built from a
# pinned Apache ADBC release by default, so building the plugin needs no conda
# environment or system package. Drivers are found at run time by path or by
# name through driver manifests; scripts/install_adbc_driver.sh installs one.
option(IBEX_ADBC_SYSTEM_DRIVER_MANAGER
    "Link an installed ADBC driver manager instead of building the bundled one" OFF)

set(_ibex_adbc_prefix_hints)
if(DEFINED ENV{CONDA_PREFIX} AND NOT "$ENV{CONDA_PREFIX}" STREQUAL "")
    list(APPEND _ibex_adbc_prefix_hints "$ENV{CONDA_PREFIX}")
endif()

if(IBEX_ADBC_SYSTEM_DRIVER_MANAGER)
    find_path(ADBC_INCLUDE_DIR
        NAMES adbc.h
        HINTS ${_ibex_adbc_prefix_hints}
        PATH_SUFFIXES include
    )
    find_library(ADBC_DRIVER_MANAGER_LIBRARY
        NAMES adbc_driver_manager libadbc_driver_manager
        HINTS ${_ibex_adbc_prefix_hints}
        PATH_SUFFIXES lib
    )
    if(NOT ADBC_INCLUDE_DIR OR NOT ADBC_DRIVER_MANAGER_LIBRARY)
        message(FATAL_ERROR
            "IBEX_ADBC_SYSTEM_DRIVER_MANAGER=ON requires an installed ADBC driver manager "
            "(missing adbc.h or libadbc_driver_manager). Activate the Conda "
            "environment that provides it, set CMAKE_PREFIX_PATH, or turn the option "
            "off to build the bundled driver manager."
        )
    endif()
    set(_ibex_adbc_include_dir "${ADBC_INCLUDE_DIR}")
    set(_ibex_adbc_manager "${ADBC_DRIVER_MANAGER_LIBRARY}")
else()
    include(FetchContent)
    # downloads.apache.org carries only the latest release; archive.apache.org
    # keeps every release. The hash is Apache's published SHA-512.
    FetchContent_Declare(arrow_adbc
        URL
            https://downloads.apache.org/arrow/apache-arrow-adbc-24/apache-arrow-adbc-24.tar.gz
            https://archive.apache.org/dist/arrow/apache-arrow-adbc-24/apache-arrow-adbc-24.tar.gz
        URL_HASH SHA512=8c89390da0d730c3376be22faadbda7405bc5e8183adea72314a315d39599db841471aaea46ca5f5540ada5d32fa0740291c0a52063cf6742b417bbcd2be52c4
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        # Only the C driver manager sources are used; skip ADBC's own project.
        SOURCE_SUBDIR _ibex_no_cmake_project
    )
    FetchContent_MakeAvailable(arrow_adbc)

    set(_adbc_c "${arrow_adbc_SOURCE_DIR}/c")
    add_library(ibex_adbc_driver_manager STATIC
        "${_adbc_c}/driver_manager/adbc_driver_manager.cc"
        "${_adbc_c}/driver_manager/adbc_driver_manager_api.cc"
        "${_adbc_c}/driver_manager/adbc_driver_manager_driver_loading.cc"
        "${_adbc_c}/driver_manager/adbc_driver_manager_profiles.cc"
    )
    target_include_directories(ibex_adbc_driver_manager SYSTEM
        PRIVATE "${_adbc_c}" "${_adbc_c}/vendor" "${_adbc_c}/driver"
        PUBLIC "${_adbc_c}/include"
    )
    target_compile_definitions(ibex_adbc_driver_manager
        # Linked statically into the plugin: no dllimport/dllexport.
        PUBLIC ADBC_EXPORT=
        # Also search $CONDA_PREFIX/etc/adbc/drivers when a conda env is active.
        PRIVATE ADBC_CONDA_BUILD=1
    )
    set_target_properties(ibex_adbc_driver_manager PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        # Keep the ADBC API out of adbc.so's exports.
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON
    )
    target_link_libraries(ibex_adbc_driver_manager PRIVATE ${CMAKE_DL_LIBS})
    if(WIN32)
        # As in ADBC's own build (cmake_modules/AdbcDefines.cmake).
        target_compile_definitions(ibex_adbc_driver_manager PRIVATE _CRT_SECURE_NO_WARNINGS)
        target_link_libraries(ibex_adbc_driver_manager PRIVATE advapi32 uuid shell32)
    endif()
    set(_ibex_adbc_include_dir "${_adbc_c}/include")
    set(_ibex_adbc_manager ibex_adbc_driver_manager)
    unset(_adbc_c)
endif()

# The SQLite driver is only needed by the integration tests (tests/test_adbc.cpp);
# the plugin itself loads whichever driver a script names at run time.
find_library(ADBC_DRIVER_SQLITE_LIBRARY
    NAMES adbc_driver_sqlite libadbc_driver_sqlite
    HINTS ${_ibex_adbc_prefix_hints}
    PATH_SUFFIXES lib
)
if(NOT ADBC_DRIVER_SQLITE_LIBRARY)
    message(STATUS "ADBC SQLite driver not found; ADBC integration tests are disabled")
endif()

unset(_ibex_adbc_prefix_hints)

set_target_properties(ibex_adbc_plugin PROPERTIES
    OUTPUT_NAME adbc
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
    RUNTIME_OUTPUT_DIRECTORY "${IBEX_PLUGIN_OUTPUT_DIR}"
)

target_include_directories(ibex_adbc_plugin
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${PROJECT_SOURCE_DIR}/include"
        "${_ibex_adbc_include_dir}"
)

target_link_libraries(ibex_adbc_plugin
    PRIVATE
        Ibex::interop
        Ibex::CompilerOptions
        "${_ibex_adbc_manager}"
)

unset(_ibex_adbc_include_dir)
unset(_ibex_adbc_manager)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_adbc_plugin PRIVATE Ibex::Sanitizers)
endif()

add_custom_command(TARGET ibex_adbc_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/adbc.ibex"
        "${IBEX_PLUGIN_OUTPUT_DIR}/adbc.ibex"
    COMMENT "Installing adbc.ibex stub"
)
