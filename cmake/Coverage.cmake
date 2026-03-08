# Coverage.cmake — LLVM source-based coverage for ibex
#
# Usage:
#   cmake -B build-coverage -G Ninja \
#         -DCMAKE_CXX_COMPILER=clang++ \
#         -DCMAKE_BUILD_TYPE=Debug \
#         -DIBEX_COVERAGE=ON
#   cmake --build build-coverage
#   cmake --build build-coverage --target coverage
#
# Outputs:
#   build-coverage/coverage/index.html   — browsable line-by-line report
#   build-coverage/coverage/summary.txt  — per-file coverage percentages
#
# How LLVM source-based coverage works:
#   1. Compiler instruments every branch with a counter at compile time.
#   2. Running the test binary writes a raw profile (.profraw).
#   3. llvm-profdata merges raw profiles into a indexed profile (.profdata).
#   4. llvm-cov renders the profile against the binary's embedded coverage
#      mappings to produce line/branch/function coverage reports.
#
# This is more accurate than gcov: instrumentation happens at the source
# AST level, not at the assembly level, so macros and templates are handled
# correctly.

option(IBEX_COVERAGE "Build with LLVM source-based coverage instrumentation" OFF)

if(NOT IBEX_COVERAGE)
    return()
endif()

message(STATUS "Coverage: LLVM source-based coverage enabled")

# Require Clang — GCC uses a different coverage format.
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "IBEX_COVERAGE requires clang++ (CMAKE_CXX_COMPILER_ID=${CMAKE_CXX_COMPILER_ID})")
endif()

# Find versioned llvm-profdata / llvm-cov in PATH or alongside the compiler.
find_program(LLVM_PROFDATA NAMES
    llvm-profdata-20 llvm-profdata-19 llvm-profdata-18 llvm-profdata
    HINTS "${LLVM_TOOLS_BINARY_DIR}"
    DOC "llvm-profdata tool")

find_program(LLVM_COV NAMES
    llvm-cov-20 llvm-cov-19 llvm-cov-18 llvm-cov
    HINTS "${LLVM_TOOLS_BINARY_DIR}"
    DOC "llvm-cov tool")

if(NOT LLVM_PROFDATA)
    message(FATAL_ERROR "llvm-profdata not found; install llvm (apt install llvm)")
endif()
if(NOT LLVM_COV)
    message(FATAL_ERROR "llvm-cov not found; install llvm (apt install llvm)")
endif()

message(STATUS "Coverage: llvm-profdata = ${LLVM_PROFDATA}")
message(STATUS "Coverage: llvm-cov      = ${LLVM_COV}")

# Instrumentation flags added to every target in this build.
add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
add_link_options(-fprofile-instr-generate)

# Convenience variables used by the custom target below.
set(IBEX_COVERAGE_PROFRAW   "${CMAKE_BINARY_DIR}/coverage.profraw")
set(IBEX_COVERAGE_PROFDATA  "${CMAKE_BINARY_DIR}/coverage.profdata")
set(IBEX_COVERAGE_REPORT_DIR "${CMAKE_BINARY_DIR}/coverage")

# The 'coverage' target:
#   1. Runs ibex_tests (or any test binary) with LLVM_PROFILE_FILE set.
#   2. Merges the raw profile into an indexed .profdata file.
#   3. Generates an HTML report and a text summary filtered to project sources.
#
# We delegate to a configure_file-generated shell script to avoid CMake/Ninja
# passing shell metacharacters (parens in the regex, pipe for tee) through
# /bin/sh unquoted.
function(ibex_add_coverage_target TEST_BINARY)
    # Stamp the binary path into the script at configure time.
    set(IBEX_TEST_BINARY "$<TARGET_FILE:${TEST_BINARY}>")

    # configure_file can't expand generator expressions, so we use a two-step:
    # first configure the script with all static values, then a second custom
    # command substitutes the generator expression for the binary path.
    set(_script_in  "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/run_coverage.sh.in")
    set(_script_out "${CMAKE_BINARY_DIR}/run_coverage.sh")

    # Variables used by run_coverage.sh.in
    configure_file("${_script_in}" "${_script_out}.tmp" @ONLY)

    # Replace the @IBEX_TEST_BINARY@ placeholder (which contains a generator
    # expression) at build time using a file(GENERATE ...).
    file(GENERATE
        OUTPUT  "${_script_out}"
        INPUT   "${_script_out}.tmp"
    )

    add_custom_target(coverage
        COMMENT "Running tests and generating LLVM coverage report..."
        COMMAND chmod +x "${_script_out}"
        COMMAND "${_script_out}"
        DEPENDS ${TEST_BINARY}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
        USES_TERMINAL
    )

    message(STATUS "Coverage: 'cmake --build . --target coverage' will generate reports in ${IBEX_COVERAGE_REPORT_DIR}")
endfunction()
