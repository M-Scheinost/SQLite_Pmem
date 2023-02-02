set(SQLITE_FILES
    ${CMAKE_SOURCE_DIR}/sqlite/sqlite/sqlite3.h
    ${CMAKE_SOURCE_DIR}/sqlite/sqlite/sqlite3.c
    ${CMAKE_SOURCE_DIR}/sqlite/sqlite/sqlite3ext.h
)

set(MSC_LOG_DENSE
    ${CMAKE_SOURCE_DIR}/sqlite/msc-log-dense/sqlite3.h
    ${CMAKE_SOURCE_DIR}/sqlite/msc-log-dense/sqlite3.c
    ${CMAKE_SOURCE_DIR}/sqlite/msc-log-dense/sqlite3ext.h
    ${CMAKE_SOURCE_DIR}/sqlite/msc-log-dense/uthash.h
    )

set(MSC_LOG_LARGE
    ${CMAKE_SOURCE_DIR}/sqlite/msc-log-large/sqlite3.h
    ${CMAKE_SOURCE_DIR}/sqlite/msc-log-large/sqlite3.c
    ${CMAKE_SOURCE_DIR}/sqlite/msc-log-large/sqlite3ext.h
    )

set(SQLITE_SHELL  ${CMAKE_SOURCE_DIR}/sqlite/sqlite/shell.c)

add_library(sqlite ${SQLITE_FILES})
target_compile_options(
        sqlite
        PRIVATE
        -DSQLITE_DQS=0
        -DSQLITE_THREADSAFE=0
        -DSQLITE_OMIT_LOAD_EXTENSION
        -DSQLITE_DEFAULT_MEMSTATUS=0
        -DSQLITE_LIKE_DOESNT_MATCH_BLOBS
        -DSQLITE_MAX_EXPR_DEPTH=0
        -DSQLITE_OMIT_DEPRECATED
        -DSQLITE_OMIT_PROGRESS_CALLBACK
        -DSQLITE_OMIT_SHARED_CACHE
        -DSQLITE_USE_ALLOCA
        -DSQLITE_OMIT_AUTOINIT
)


add_library(msclogdense ${MSC_LOG_DENSE})
target_compile_options(
        msclogdense
        PRIVATE
        -DSQLITE_DQS=0
        -DSQLITE_THREADSAFE=0
        -DSQLITE_OMIT_LOAD_EXTENSION
        -DSQLITE_DEFAULT_MEMSTATUS=0
        -DSQLITE_LIKE_DOESNT_MATCH_BLOBS
        -DSQLITE_MAX_EXPR_DEPTH=0
        -DSQLITE_OMIT_DEPRECATED
        -DSQLITE_OMIT_PROGRESS_CALLBACK
        -DSQLITE_OMIT_SHARED_CACHE
        -DSQLITE_USE_ALLOCA
        -DSQLITE_OMIT_AUTOINIT
)

add_library(mscloglarge ${MSC_LOG_DENSE})
target_compile_options(
        mscloglarge
        PRIVATE
        -DSQLITE_DQS=0
        -DSQLITE_THREADSAFE=0
        -DSQLITE_OMIT_LOAD_EXTENSION
        -DSQLITE_DEFAULT_MEMSTATUS=0
        -DSQLITE_LIKE_DOESNT_MATCH_BLOBS
        -DSQLITE_MAX_EXPR_DEPTH=0
        -DSQLITE_OMIT_DEPRECATED
        -DSQLITE_OMIT_PROGRESS_CALLBACK
        -DSQLITE_OMIT_SHARED_CACHE
        -DSQLITE_USE_ALLOCA
        -DSQLITE_OMIT_AUTOINIT
)