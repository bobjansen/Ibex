add_library(ibex_repl STATIC
    repl.cpp
)
add_library(Ibex::repl ALIAS ibex_repl)

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

target_include_directories(ibex_repl
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(ibex_repl
    PUBLIC
        Ibex::parser
        Ibex::runtime
    PRIVATE
        Ibex::CompilerOptions
        fmt::fmt
        spdlog::spdlog
        dl
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_repl PRIVATE Ibex::Sanitizers)
endif()

if(Readline_FOUND)
    target_compile_definitions(ibex_repl PRIVATE IBEX_HAS_READLINE=1)
    if(TARGET Readline::Readline)
        target_link_libraries(ibex_repl PRIVATE Readline::Readline)
    elseif(DEFINED Readline_LIBRARIES)
        target_link_libraries(ibex_repl PRIVATE ${Readline_LIBRARIES})
        if(DEFINED Readline_INCLUDE_DIRS)
            target_include_directories(ibex_repl PRIVATE ${Readline_INCLUDE_DIRS})
        endif()
    endif()
endif()
