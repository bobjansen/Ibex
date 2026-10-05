# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# The evaluator and its session API, with no terminal dependency, compiled once
# as objects. It calls the line-editing functions in line_editing.hpp, which
# each of the two libraries below pairs with those objects, so either archive
# is complete on its own (no link-order dependency between them):
#   ibex_repl      - plain std::getline line reader. What every embedder links
#                    (tests, ibex_eval, the UI server, the R package, wasm).
#   ibex_repl_cli  - GNU readline / libedit: history, completion, Ctrl+C at the
#                    prompt. Only the interactive `ibex` binary links it.
add_library(ibex_repl_core OBJECT
    repl.cpp
)

target_include_directories(ibex_repl_core
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(ibex_repl_core
    PUBLIC
        Ibex::parser
        Ibex::runtime
    PRIVATE
        Ibex::CompilerOptions
        $<$<NOT:$<PLATFORM_ID:Windows>>:dl>
)

add_library(ibex_repl STATIC
    line_editing_plain.cpp
)
add_library(Ibex::repl ALIAS ibex_repl)
target_link_libraries(ibex_repl
    PUBLIC
        ibex_repl_core
    PRIVATE
        Ibex::CompilerOptions
)

foreach(target ibex_repl_core ibex_repl)
    if(TARGET Ibex::Sanitizers)
        target_link_libraries(${target} PRIVATE Ibex::Sanitizers)
    endif()
endforeach()

find_package(Readline QUIET)
if(NOT Readline_FOUND)
    find_path(IBEX_READLINE_INCLUDE_DIR readline/readline.h)
    find_library(IBEX_READLINE_LIBRARY NAMES readline)
    if(IBEX_READLINE_INCLUDE_DIR AND IBEX_READLINE_LIBRARY)
        set(Readline_FOUND TRUE)
        set(Readline_INCLUDE_DIRS "${IBEX_READLINE_INCLUDE_DIR}")
        set(Readline_LIBRARIES "${IBEX_READLINE_LIBRARY}")
    endif()
endif()

if(Readline_FOUND)
    add_library(ibex_repl_cli STATIC
        line_editing_readline.cpp
    )
    target_link_libraries(ibex_repl_cli
        PUBLIC
            ibex_repl_core
        PRIVATE
            Ibex::CompilerOptions
    )
    if(TARGET Readline::Readline)
        target_link_libraries(ibex_repl_cli PRIVATE Readline::Readline)
    elseif(DEFINED Readline_LIBRARIES)
        target_link_libraries(ibex_repl_cli PRIVATE ${Readline_LIBRARIES})
        if(DEFINED Readline_INCLUDE_DIRS)
            target_include_directories(ibex_repl_cli PRIVATE ${Readline_INCLUDE_DIRS})
        endif()
    endif()
    if(TARGET Ibex::Sanitizers)
        target_link_libraries(ibex_repl_cli PRIVATE Ibex::Sanitizers)
    endif()
else()
    # No readline: the interactive binary gets the plain reader too.
    add_library(ibex_repl_cli ALIAS ibex_repl)
endif()
add_library(Ibex::repl_cli ALIAS ibex_repl_cli)
