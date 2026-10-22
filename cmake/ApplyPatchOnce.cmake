# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Apply PATCH_FILE to the git checkout in the current directory, once.
#
# Run as a FetchContent PATCH_COMMAND:
#   ${CMAKE_COMMAND} -DPATCH_FILE=<file> -P ApplyPatchOnce.cmake
#
# FetchContent can rerun the patch step over a tree it already patched (any
# change to the declaration reruns its sub-build), so an already-applied patch
# is a success. A patch that neither applies nor is already present -- the
# upstream source moved under it -- is a hard error rather than a build that
# silently lacks the change.
if(NOT DEFINED PATCH_FILE)
    message(FATAL_ERROR "ApplyPatchOnce.cmake: PATCH_FILE is not set")
endif()
find_package(Git REQUIRED)
execute_process(
    COMMAND "${GIT_EXECUTABLE}" apply --reverse --check "${PATCH_FILE}"
    RESULT_VARIABLE _ibex_already_applied
    OUTPUT_QUIET ERROR_QUIET)
if(_ibex_already_applied EQUAL 0)
    message(STATUS "Patch already applied: ${PATCH_FILE}")
    return()
endif()
execute_process(
    COMMAND "${GIT_EXECUTABLE}" apply "${PATCH_FILE}"
    RESULT_VARIABLE _ibex_apply_result
    ERROR_VARIABLE _ibex_apply_error)
if(NOT _ibex_apply_result EQUAL 0)
    message(FATAL_ERROR "Failed to apply ${PATCH_FILE}:\n${_ibex_apply_error}")
endif()
message(STATUS "Applied patch: ${PATCH_FILE}")
