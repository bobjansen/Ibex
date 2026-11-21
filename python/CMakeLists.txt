# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Select a single Python installation before asking FindPython for its headers
# and extension-module support.  In particular, do not force the repository
# venv when a Conda or virtualenv environment is active: doing so can pair the
# venv interpreter with the active environment's headers and libraries.
set(IBEX_PYTHON_EXECUTABLE "" CACHE FILEPATH
    "Python interpreter used to build and test the Ibex pyarrow bridge")
set(IBEX_POLARS_UPSTREAM_DIR "" CACHE PATH
    "Optional unmodified Polars 2.0.0 source tree for upstream conformance tests")

if(IBEX_PYTHON_EXECUTABLE)
    set(Python3_EXECUTABLE "${IBEX_PYTHON_EXECUTABLE}")
elseif(DEFINED ENV{CONDA_PREFIX} AND EXISTS "$ENV{CONDA_PREFIX}/bin/python")
    set(Python3_ROOT_DIR "$ENV{CONDA_PREFIX}")
    set(Python3_FIND_VIRTUALENV ONLY)
elseif(DEFINED ENV{VIRTUAL_ENV} AND EXISTS "$ENV{VIRTUAL_ENV}/bin/python")
    set(Python3_ROOT_DIR "$ENV{VIRTUAL_ENV}")
    set(Python3_FIND_VIRTUALENV ONLY)
else()
    set(IBEX_VENV_PYTHON "${PROJECT_SOURCE_DIR}/.venv/bin/python")
    if(EXISTS "${IBEX_VENV_PYTHON}")
        set(Python3_EXECUTABLE "${IBEX_VENV_PYTHON}")
    endif()
endif()

find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)

Python3_add_library(ibex_pyarrow MODULE WITH_SOABI
    ibex_pyarrow_module.cpp
)

target_link_libraries(ibex_pyarrow
    PRIVATE
        Ibex::parser
        Ibex::runtime
        Ibex::repl
        Ibex::interop
        Ibex::CompilerOptions
        Python3::Module
)

set(IBEX_PYTHON_HAS_PARQUET 0)
if(TARGET Ibex::parquet)
    set(IBEX_PYTHON_HAS_PARQUET 1)
    target_link_libraries(ibex_pyarrow PRIVATE Ibex::parquet)
    target_compile_definitions(ibex_pyarrow PRIVATE IBEX_HAS_PARQUET_BACKEND=1)

    # The Parquet plugin copies Arrow's shared libraries into the plugin output
    # directory. The extension links Arrow through Ibex::parquet, so its build
    # tree RPATH must name that directory as well; otherwise Python cannot
    # import ibex_pyarrow without callers manually setting LD_LIBRARY_PATH.
    set_target_properties(ibex_pyarrow PROPERTIES
        BUILD_RPATH "${IBEX_PLUGIN_OUTPUT_DIR}"
    )
endif()

target_include_directories(ibex_pyarrow
    PRIVATE
        "${PROJECT_SOURCE_DIR}/include"
)

target_compile_definitions(ibex_pyarrow
    PRIVATE
        PY_SSIZE_T_CLEAN
)

set_target_properties(ibex_pyarrow PROPERTIES
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/python"
)

install(TARGETS ibex_pyarrow
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)

# ── Wheel layout: the `python` install component ─────────────────────────────
# scikit-build-core (python/pyproject.toml) installs only this component into
# the wheel root, beside the pure-Python `ibex` and `ibex_polars` packages:
#
#   ibex_pyarrow.<soabi>.so
#   ibex_pyarrow.libs/       Arrow/Parquet shared libraries, by SONAME
#
# The extension finds them through its RPATH and the Arrow libraries find one
# another via ARROW_RPATH_ORIGIN (libs/parquet). Windows has no RPATH, so the
# DLLs go beside the extension, where the loader searches.
if(WIN32)
    set(_ibex_wheel_libdir ".")
else()
    set(_ibex_wheel_libdir "ibex_pyarrow.libs")
    if(APPLE)
        set(_ibex_wheel_rpath "@loader_path/${_ibex_wheel_libdir}")
    else()
        set(_ibex_wheel_rpath "$ORIGIN/${_ibex_wheel_libdir}")
    endif()
    set_target_properties(ibex_pyarrow PROPERTIES INSTALL_RPATH "${_ibex_wheel_rpath}")
    unset(_ibex_wheel_rpath)
endif()
install(TARGETS ibex_pyarrow
    LIBRARY DESTINATION "." COMPONENT python EXCLUDE_FROM_ALL
    RUNTIME DESTINATION "." COMPONENT python EXCLUDE_FROM_ALL
)
if(TARGET Ibex::parquet)
    foreach(_ibex_arrow_target arrow_shared parquet_shared arrow_ipc_shared)
        if(NOT TARGET ${_ibex_arrow_target})
            continue()
        endif()
        get_target_property(_ibex_arrow_type ${_ibex_arrow_target} TYPE)
        if(NOT _ibex_arrow_type STREQUAL "SHARED_LIBRARY")
            continue()
        endif()
        if(WIN32)
            install(TARGETS ${_ibex_arrow_target}
                RUNTIME DESTINATION "${_ibex_wheel_libdir}" COMPONENT python EXCLUDE_FROM_ALL
            )
        else()
            install(TARGETS ${_ibex_arrow_target}
                LIBRARY DESTINATION "${_ibex_wheel_libdir}" COMPONENT python
                    NAMELINK_SKIP EXCLUDE_FROM_ALL
            )
            # A wheel cannot hold symlinks, so the SONAME link would ship as a
            # second copy of the library. Keep one file, under the SONAME the
            # loader looks up (libarrow.so.2200).
            install(CODE "
                set(_dir \"\${CMAKE_INSTALL_PREFIX}/${_ibex_wheel_libdir}\")
                set(_real \"\${_dir}/$<TARGET_FILE_NAME:${_ibex_arrow_target}>\")
                set(_soname \"\${_dir}/$<TARGET_SONAME_FILE_NAME:${_ibex_arrow_target}>\")
                if(NOT _real STREQUAL _soname)
                    file(REMOVE \"\${_soname}\")
                    file(RENAME \"\${_real}\" \"\${_soname}\")
                endif()
                " COMPONENT python EXCLUDE_FROM_ALL)
        endif()
    endforeach()
    unset(_ibex_arrow_target)
    unset(_ibex_arrow_type)
endif()
# Python ships vcruntime140*.dll but not the C++ library (msvcp140.dll).
if(MSVC)
    set(CMAKE_INSTALL_SYSTEM_RUNTIME_DESTINATION "${_ibex_wheel_libdir}")
    set(CMAKE_INSTALL_SYSTEM_RUNTIME_COMPONENT python)
    set(CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_SKIP ON)
    include(InstallRequiredSystemLibraries)
    foreach(_ibex_msvc_dll IN LISTS CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS)
        if(_ibex_msvc_dll MATCHES "msvcp140[^/\\]*\\.dll$")
            install(PROGRAMS "${_ibex_msvc_dll}" DESTINATION "${_ibex_wheel_libdir}"
                COMPONENT python EXCLUDE_FROM_ALL)
        endif()
    endforeach()
    unset(_ibex_msvc_dll)
endif()
unset(_ibex_wheel_libdir)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_pyarrow PRIVATE Ibex::Sanitizers)
endif()

execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c "import pyarrow"
    RESULT_VARIABLE IBEX_PYARROW_IMPORT_RESULT
    OUTPUT_QUIET
    ERROR_QUIET
)

execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c "import IPython"
    RESULT_VARIABLE IBEX_IPYTHON_IMPORT_RESULT
    OUTPUT_QUIET
    ERROR_QUIET
)

if(IBEX_BUILD_TESTS AND IBEX_PYARROW_IMPORT_RESULT EQUAL 0)
    execute_process(
        COMMAND "${Python3_EXECUTABLE}" -c "import polars; assert polars.__version__ == '2.0.0'"
        RESULT_VARIABLE IBEX_POLARS_IMPORT_RESULT
        OUTPUT_QUIET ERROR_QUIET
    )
    if(IBEX_POLARS_IMPORT_RESULT EQUAL 0)
        add_test(
            NAME ibex_polars_engine
            COMMAND "${CMAKE_COMMAND}" -E env
                "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
                "IBEX_TEST_PARQUET=${IBEX_PYTHON_HAS_PARQUET}"
                "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_polars.py"
        )
        add_test(
            NAME ibex_polars_temporal
            COMMAND "${CMAKE_COMMAND}" -E env
                "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
                "IBEX_TEST_PARQUET=${IBEX_PYTHON_HAS_PARQUET}"
                "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_temporal.py"
        )
        add_test(
            NAME ibex_polars_first_last
            COMMAND "${CMAKE_COMMAND}" -E env
                "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
                "IBEX_TEST_PARQUET=${IBEX_PYTHON_HAS_PARQUET}"
                "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_first_last.py"
        )
        add_test(
            NAME ibex_polars_joins
            COMMAND "${CMAKE_COMMAND}" -E env
                "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
                "IBEX_TEST_PARQUET=${IBEX_PYTHON_HAS_PARQUET}"
                "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_joins.py"
        )
        add_test(
            NAME ibex_polars_slices
            COMMAND "${CMAKE_COMMAND}" -E env
                "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
                "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_slices.py"
        )
        execute_process(
            COMMAND "${Python3_EXECUTABLE}" -c
                "import pytest, hypothesis; assert pytest.__version__ == '9.0.3'; assert hypothesis.__version__ == '6.170.9'"
            RESULT_VARIABLE IBEX_POLARS_TEST_DEPS_RESULT
            OUTPUT_QUIET ERROR_QUIET
        )
        if(IBEX_POLARS_TEST_DEPS_RESULT EQUAL 0)
            add_test(
                NAME ibex_polars_conformance_harness
                COMMAND "${CMAKE_COMMAND}" -E env
                    "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
                    "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_polars_conformance.py"
            )
        endif()
        if(IBEX_POLARS_UPSTREAM_DIR)
            if(NOT IBEX_POLARS_TEST_DEPS_RESULT EQUAL 0)
                message(FATAL_ERROR
                    "Upstream conformance requires python/conformance/requirements.txt in ${Python3_EXECUTABLE}")
            endif()
            add_test(
                NAME ibex_polars_upstream
                COMMAND "${CMAKE_COMMAND}" -E env
                    "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
                    "${Python3_EXECUTABLE}" -m conformance.run
                    --upstream "${IBEX_POLARS_UPSTREAM_DIR}"
                    --report-dir "${PROJECT_BINARY_DIR}/polars-conformance-results"
            )
            set_tests_properties(ibex_polars_upstream PROPERTIES LABELS "python;conformance" TIMEOUT 300)
        endif()
    elseif(IBEX_POLARS_UPSTREAM_DIR)
        message(FATAL_ERROR "Upstream conformance requires Polars 2.0.0")
    endif()
    add_test(
        NAME ibex_python_lazy
        COMMAND "${CMAKE_COMMAND}" -E env
            "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
            "IBEX_TEST_PARQUET=${IBEX_PYTHON_HAS_PARQUET}"
            "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_lazy.py"
    )
    add_test(
        NAME ibex_python_parquet_footer_cache
        COMMAND "${CMAKE_COMMAND}" -E env
            "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
            "IBEX_TEST_PARQUET=${IBEX_PYTHON_HAS_PARQUET}"
            "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_parquet_footer_cache.py"
    )
    add_test(
        NAME ibex_pyarrow_smoke
        COMMAND "${CMAKE_COMMAND}" -E env
            "PYTHONPATH=${PROJECT_BINARY_DIR}/python"
            "IBEX_PYARROW_MODULE_DIR=${PROJECT_BINARY_DIR}/python"
            "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_pyarrow.py"
    )
elseif(IBEX_BUILD_TESTS)
    if(IBEX_POLARS_UPSTREAM_DIR)
        message(FATAL_ERROR "Upstream conformance requires PyArrow in ${Python3_EXECUTABLE}")
    endif()
    message(STATUS
        "Skipping ibex_pyarrow_smoke: pyarrow is not importable in ${Python3_EXECUTABLE}")
endif()

if(IBEX_BUILD_TESTS AND IBEX_PYARROW_IMPORT_RESULT EQUAL 0 AND IBEX_IPYTHON_IMPORT_RESULT EQUAL 0)
    add_test(
        NAME ibex_ipython_smoke
        COMMAND "${CMAKE_COMMAND}" -E env
            "PYTHONPATH=${PROJECT_BINARY_DIR}/python:${PROJECT_SOURCE_DIR}/python"
            "IBEX_PYARROW_MODULE_DIR=${PROJECT_BINARY_DIR}/python"
            "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_ipython.py"
    )
elseif(IBEX_BUILD_TESTS)
    message(STATUS
        "Skipping ibex_ipython_smoke: IPython or pyarrow is not importable in ${Python3_EXECUTABLE}")
endif()
