set(IBEX_VENV_PYTHON "${PROJECT_SOURCE_DIR}/.venv/bin/python")
if(EXISTS "${IBEX_VENV_PYTHON}")
    set(Python3_EXECUTABLE "${IBEX_VENV_PYTHON}" CACHE FILEPATH
        "Preferred Python interpreter for the Ibex pyarrow bridge" FORCE)
endif()

find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)

Python3_add_library(ibex_pyarrow MODULE WITH_SOABI
    ibex_pyarrow_module.cpp
)

target_link_libraries(ibex_pyarrow
    PRIVATE
        Ibex::parser
        Ibex::runtime
        Ibex::interop
        Ibex::CompilerOptions
        Python3::Module
)

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

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_pyarrow PRIVATE Ibex::Sanitizers)
endif()

execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c "import pyarrow"
    RESULT_VARIABLE IBEX_PYARROW_IMPORT_RESULT
    OUTPUT_QUIET
    ERROR_QUIET
)

if(IBEX_BUILD_TESTS AND IBEX_PYARROW_IMPORT_RESULT EQUAL 0)
    add_test(
        NAME ibex_pyarrow_smoke
        COMMAND "${CMAKE_COMMAND}" -E env
            "PYTHONPATH=${PROJECT_BINARY_DIR}/python"
            "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/python/test_ibex_pyarrow.py"
    )
elseif(IBEX_BUILD_TESTS)
    message(STATUS
        "Skipping ibex_pyarrow_smoke: pyarrow is not importable in ${Python3_EXECUTABLE}")
endif()
