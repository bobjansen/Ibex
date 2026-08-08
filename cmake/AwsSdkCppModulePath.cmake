# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Keep aws-sdk-cpp's generic include(dependencies) resolving to its own helper.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/cmake/dependencies.cmake")
    list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
endif()
