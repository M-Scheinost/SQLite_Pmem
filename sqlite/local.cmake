set(SQLITE_FILES
    ${CMAKE_SOURCE_DIR}/sqlite/sqlite3.h
    ${CMAKE_SOURCE_DIR}/sqlite/sqlite3.c
    ${CMAKE_SOURCE_DIR}/sqlite/sqlite3ext.h
)

add_library(sqlite ${SQLITE_FILES})