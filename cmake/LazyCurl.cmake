# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# libcurl for the plugins' HTTPS readers (csv::read, parquet::read), loaded on
# first use rather than linked.
#
# Linked, libcurl and its ~30-library dependency chain (TLS, Kerberos, LDAP,
# SSH, ...) cost every process ~2.5 ms of start-up -- and the csv plugin is
# loaded by any script that declares csv::write, so that was every PDS-H query.
# With IBEX_LAZY_CURL a target compiles against libcurl's headers only and
# libs/common/curl_api.hpp dlopens it by SONAME the first time an https:// path
# is read. The SONAME is read from the library CMake found, so the runtime load
# matches the headers compiled against.
#
# Requires CURL::libcurl (find_package(CURL)) before ibex_link_curl is called.

include_guard(GLOBAL)

if(UNIX AND NOT APPLE)
    set(_ibex_lazy_curl_default ON)
else()
    set(_ibex_lazy_curl_default OFF)
endif()
option(IBEX_LAZY_CURL "Load libcurl on first HTTPS use instead of linking it (ELF only)"
       ${_ibex_lazy_curl_default})
if(IBEX_LAZY_CURL AND (APPLE OR NOT UNIX))
    message(FATAL_ERROR "IBEX_LAZY_CURL is supported on ELF platforms only")
endif()

# The SONAME of the libcurl CURL::libcurl names, cached in the global property
# IBEX_LAZY_CURL_SONAME. Fails configure if it cannot be read: a lazy build that
# guessed the name would only fail at the first https:// read.
function(_ibex_lazy_curl_soname out_var)
    get_property(_soname GLOBAL PROPERTY IBEX_LAZY_CURL_SONAME)
    if(_soname)
        set(${out_var} "${_soname}" PARENT_SCOPE)
        return()
    endif()
    set(_library "${CURL_LIBRARY}")
    if(NOT _library)
        set(_library "${CURL_LIBRARY_RELEASE}")
    endif()
    if(NOT _library)
        foreach(_prop IMPORTED_LOCATION IMPORTED_LOCATION_RELEASE IMPORTED_LOCATION_NOCONFIG)
            get_target_property(_library CURL::libcurl ${_prop})
            if(_library)
                break()
            endif()
        endforeach()
    endif()
    if(_library AND CMAKE_OBJDUMP)
        execute_process(COMMAND "${CMAKE_OBJDUMP}" -p "${_library}"
                        OUTPUT_VARIABLE _dump ERROR_QUIET)
        string(REGEX MATCH "SONAME +([^ \n]+)" _match "${_dump}")
        set(_soname "${CMAKE_MATCH_1}")
    endif()
    if(NOT _soname)
        message(FATAL_ERROR "IBEX_LAZY_CURL: cannot read libcurl's SONAME from '${_library}' "
                            "(set -DIBEX_LAZY_CURL=OFF to link it instead)")
    endif()
    message(STATUS "HTTPS readers load ${_soname} on first use")
    set_property(GLOBAL PROPERTY IBEX_LAZY_CURL_SONAME "${_soname}")
    set(${out_var} "${_soname}" PARENT_SCOPE)
endfunction()

# Give `target` libcurl: linked, or (IBEX_LAZY_CURL) its headers plus the
# IBEX_LAZY_CURL_SONAME definition curl_api.hpp loads it by.
function(ibex_link_curl target)
    if(IBEX_LAZY_CURL)
        _ibex_lazy_curl_soname(_soname)
        target_include_directories(${target} SYSTEM PRIVATE
            "$<TARGET_PROPERTY:CURL::libcurl,INTERFACE_INCLUDE_DIRECTORIES>")
        target_compile_definitions(${target} PRIVATE IBEX_LAZY_CURL_SONAME="${_soname}")
        target_link_libraries(${target} PRIVATE ${CMAKE_DL_LIBS})
    else()
        target_link_libraries(${target} PRIVATE CURL::libcurl)
    endif()
endfunction()
