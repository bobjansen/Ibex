# Keep aws-sdk-cpp's generic include(dependencies) resolving to its own helper.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/cmake/dependencies.cmake")
    list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
endif()
