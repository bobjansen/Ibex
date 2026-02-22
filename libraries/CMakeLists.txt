add_library(ibex_csv_plugin SHARED csv.cpp)

set_target_properties(ibex_csv_plugin PROPERTIES
    OUTPUT_NAME csv
    PREFIX ""
)

target_include_directories(ibex_csv_plugin
    PRIVATE
        "${PROJECT_SOURCE_DIR}/libraries"
        "${PROJECT_SOURCE_DIR}/include"
)

target_link_libraries(ibex_csv_plugin
    PRIVATE
        Ibex::runtime
        Ibex::CompilerOptions
)

if(TARGET Ibex::Sanitizers)
    target_link_libraries(ibex_csv_plugin PRIVATE Ibex::Sanitizers)
endif()

if(TARGET arrow_shared OR TARGET Arrow::arrow_shared OR TARGET arrow_static OR TARGET Arrow::arrow_static)
    add_library(ibex_parquet_plugin SHARED parquet.cpp)

    set_target_properties(ibex_parquet_plugin PROPERTIES
        OUTPUT_NAME parquet
        PREFIX ""
    )

    target_include_directories(ibex_parquet_plugin
        PRIVATE
            "${PROJECT_SOURCE_DIR}/libraries"
            "${PROJECT_SOURCE_DIR}/include"
    )
    if(DEFINED arrow_SOURCE_DIR AND DEFINED arrow_BINARY_DIR)
        target_include_directories(ibex_parquet_plugin SYSTEM
            PRIVATE
                "${arrow_SOURCE_DIR}/cpp/src"
                "${arrow_SOURCE_DIR}/cpp/src/generated"
                "${arrow_BINARY_DIR}/src"
        )
    endif()

    if(TARGET Arrow::arrow_shared)
        target_link_libraries(ibex_parquet_plugin PRIVATE Arrow::arrow_shared Arrow::parquet_shared)
        if(TARGET Arrow::arrow_ipc_shared)
            target_link_libraries(ibex_parquet_plugin PRIVATE Arrow::arrow_ipc_shared)
        endif()
    elseif(TARGET arrow_shared)
        target_link_libraries(ibex_parquet_plugin PRIVATE arrow_shared parquet_shared)
        if(TARGET arrow_ipc_shared)
            target_link_libraries(ibex_parquet_plugin PRIVATE arrow_ipc_shared)
        endif()
    elseif(TARGET Arrow::arrow_static)
        target_link_libraries(ibex_parquet_plugin PRIVATE Arrow::arrow_static Arrow::parquet_static)
    else()
        target_link_libraries(ibex_parquet_plugin PRIVATE arrow_static parquet_static)
    endif()

    target_link_libraries(ibex_parquet_plugin
        PRIVATE
            Ibex::runtime
            Ibex::CompilerOptions
    )

    if(TARGET Ibex::Sanitizers)
        target_link_libraries(ibex_parquet_plugin PRIVATE Ibex::Sanitizers)
    endif()

    # Ensure the plugin can locate Arrow shared libraries at runtime.
    set(_ibex_arrow_rpath "")
    if(TARGET Arrow::arrow_shared)
        list(APPEND _ibex_arrow_rpath
            "$<TARGET_FILE_DIR:Arrow::arrow_shared>"
            "$<TARGET_FILE_DIR:Arrow::parquet_shared>"
        )
    elseif(TARGET arrow_shared)
        list(APPEND _ibex_arrow_rpath
            "$<TARGET_FILE_DIR:arrow_shared>"
            "$<TARGET_FILE_DIR:parquet_shared>"
        )
    endif()
    if(_ibex_arrow_rpath)
        list(REMOVE_DUPLICATES _ibex_arrow_rpath)
        set_target_properties(ibex_parquet_plugin PROPERTIES
            BUILD_RPATH "${_ibex_arrow_rpath}"
        )
    endif()
endif()
