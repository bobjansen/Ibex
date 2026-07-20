# Bundled Thrift's DefineOptions.cmake does:
#   if(ZLIB_LIBRARY)
#       file(TO_CMAKE_PATH ${ZLIB_LIBRARY} ZLIB_LIBRARY)
#   endif()
# file(TO_CMAKE_PATH <path> <out-var>) takes exactly one path argument, but on
# a multi-config generator under the vcpkg toolchain, find_package(ZLIB) sets
# ZLIB_LIBRARY to the list form "optimized;<path>;debug;<path>" -- unquoted,
# that expands to 4 extra arguments and file() hard-errors ("must be called
# with exactly three arguments"). This isn't Arrow's own dependency
# resolution; ZLIB_LIBRARY ends up cached earlier in the configure (e.g. via
# find_package(CURL) transitively finding ZLIB for libs/csv) and Thrift reads
# whatever is already in the cache regardless of Arrow's ZLIB_SOURCE setting.
#
# Registered as CMAKE_PROJECT_thrift_INCLUDE_BEFORE, this runs the instant
# Thrift's own project(thrift ...) call executes -- before it includes
# DefineOptions.cmake -- so it normalizes the cache value right at the point
# of use, independent of whatever set it or when.
if(ZLIB_LIBRARY)
    list(LENGTH ZLIB_LIBRARY _ibex_zlib_lib_len)
    if(_ibex_zlib_lib_len GREATER 1)
        list(FIND ZLIB_LIBRARY "optimized" _ibex_zlib_opt_idx)
        if(_ibex_zlib_opt_idx GREATER -1)
            math(EXPR _ibex_zlib_opt_path_idx "${_ibex_zlib_opt_idx} + 1")
            list(GET ZLIB_LIBRARY ${_ibex_zlib_opt_path_idx} _ibex_zlib_single_path)
        else()
            list(GET ZLIB_LIBRARY 0 _ibex_zlib_single_path)
        endif()
        set(ZLIB_LIBRARY "${_ibex_zlib_single_path}" CACHE FILEPATH "" FORCE)
        unset(_ibex_zlib_single_path)
        unset(_ibex_zlib_opt_path_idx)
    endif()
    unset(_ibex_zlib_opt_idx)
    unset(_ibex_zlib_lib_len)
endif()
