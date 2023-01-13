#--------------------------------------------
#   for sqlite build see:
#   source: https://github.com/snikulov/sqlite.cmake.build/blob/master/CMakeLists.txt
#--------------------------------------------
#--------------------------------------------
# SQLite build script for amalgamation
cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

set(PROJECT_NAME sqlite)

# define project for C language
project(${PROJECT_NAME} C CXX)

message("Building on: ${CMAKE_SYSTEM_NAME}")

option(SQLITE_BUILD_STATIC  "Build SQLite static library" ON)
option(SQLITE_BUILD_SHARED  "Build SQLite shared library" ON)
option(WITH_SQLITE_DEBUG    "Build SQLite debug features" OFF)
option(WITH_SQLITE_MEMDEBUG "Build SQLite memory debug features" OFF)
option(WITH_SQLITE_RTREE    "Build R*Tree index extension" OFF)

set(INSTALL_BIN_DIR "${CMAKE_INSTALL_PREFIX}/bin" CACHE PATH "Installation directory for executables")
set(INSTALL_LIB_DIR "${CMAKE_INSTALL_PREFIX}/lib" CACHE PATH "Installation directory for libraries")
set(INSTALL_INC_DIR "${CMAKE_INSTALL_PREFIX}/include" CACHE PATH "Installation directory for headers")
set(CMAKE_C_STANDARD 11)
# SET (CMAKE_C_COMPILER  "/usr/bin/gcc")
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS_RELEASE "-DNDEBUG -O3")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -D_GLIBC_DEBUG")

set(SQLITE_WIN_BUILD OFF)
set(SQLITE_WINRT_BUILD OFF)

set(CMAKE_CXX_STANDARD 17)

# ---------------------
# Version detection
# ---------------------
file(STRINGS "${PROJECT_SOURCE_DIR}/sqlite/sqlite3.h" _sqlite_api_h_VER_STRING REGEX ".*#define[ ]+SQLITE_VERSION[ ]+")

string(REGEX MATCH "[0-9\\.]+" SQLITE_VER_STRING ${_sqlite_api_h_VER_STRING})
string(REGEX MATCHALL "[0-9]+" _sqlite_ver_LIST "${SQLITE_VER_STRING}")
list(LENGTH _sqlite_ver_LIST _sqlite_list_len)
list(GET _sqlite_ver_LIST 0 SQLITE_VER_MAJOR)
list(GET _sqlite_ver_LIST 1 SQLITE_VER_MINOR)
list(GET _sqlite_ver_LIST 2 SQLITE_VER_PATCH)
if(_sqlite_list_len EQUAL 4)
    list(GET _sqlite_ver_LIST 3 SQLITE_VER_PATCHLEVEL)
    message("Patch level: ${SQLITE_VER_PATCHLEVEL}")
endif()

message("FOUND: SQLite version = ${SQLITE_VER_STRING}")

set(SQLITE_DYN_NAME "${PROJECT_NAME}${SQLITE_VER_MAJOR}")
set(SQLITE_STATIC_NAME "${SQLITE_DYN_NAME}-static")

#-----------------------------------------------
# Includes
#-----------------------------------------------
find_package(Threads REQUIRED)

# add include path for project
include(${CMAKE_SOURCE_DIR}/benchmark/local.cmake)
include(${CMAKE_SOURCE_DIR}/sqlite/local.cmake)
include(${CMAKE_SOURCE_DIR}/vfs/local.cmake)
include(${CMAKE_SOURCE_DIR}/duckdb/local.cmake)

add_library(duckdb
        ${CMAKE_SOURCE_DIR}/duckdb/duckdb.cpp
        ${CMAKE_SOURCE_DIR}/duckdb/duckdb.hpp
)
target_link_libraries(duckdb Threads::Threads ${CMAKE_DL_LIBS})

#-------------------------------------------
#       Fetch git stuff
#-------------------------------------------

include(FetchContent)
FetchContent_Declare(
        cxxopts
        GIT_REPOSITORY https://github.com/jarro2783/cxxopts.git
        GIT_TAG v2.2.1
)
set(CXXOPTS_BUILD_EXAMPLES OFF)
set(CXXOPTS_BUILD_TESTS OFF)
set(CXXOPTS_ENABLE_INSTALL OFF)
set(CXXOPTS_ENABLE_WARNINGS OFF)
FetchContent_MakeAvailable(cxxopts)

FetchContent_Declare(
        dbbench
        GIT_REPOSITORY https://github.com/kpgaffney/dbbench.git
        GIT_TAG main
)
set(DBBENCH_BUILD_EXAMPLES OFF)
FetchContent_MakeAvailable(dbbench)

#----------------------------------------------
#   build shell executable
#----------------------------------------------
add_executable(sqlite3_shell ${SQLITE_SHELL})
target_link_libraries(sqlite3_shell ${SQLITE_FILES} Threads::Threads dl)
target_compile_options(sqlite3_shell PRIVATE -DSQLITE_THREADSAFE=0 -DSQLITE_OMIT_LOAD_EXTENSION)

#----------------------------------------------
#   build TATP bench executable
#----------------------------------------------

add_executable(tatp_sqlite ${TATP_SQLITE_MAIN_FILE})
target_link_libraries(tatp_sqlite ${VFS_FILES} sqlite vfs Threads::Threads dbbench_tatp cxxopts pmem pmemlog dl m)
set_target_properties(tatp_sqlite PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tatp
)

add_executable(tatp_duckdb ${TATP_DUCKDB_MAIN_FILE})
target_include_directories(tatp_duckdb PRIVATE src)
target_link_libraries(tatp_duckdb cxxopts dbbench_tatp duckdb)
set_target_properties(tatp_duckdb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/tatp)

#----------------------------------------------
#   build SSB executables.
#----------------------------------------------

add_subdirectory(benchmark/ssb/dbgen)

#       add_executable(ssb_sqlite3 ${SSB_SQLITE_MAIN_FILE})
#       
#       target_link_libraries(ssb_sqlite3 cxxopts sqlite sqlite3cpp)
#       set_target_properties(
#               ssb_sqlite3
#               PROPERTIES
#               RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/ssb
#       )

add_executable(ssb_duckdb ${SSB_DUCKDB_MAIN_FILE})
target_include_directories(ssb_duckdb PRIVATE src)
target_link_libraries(ssb_duckdb cxxopts ${CMAKE_DL_LIBS} duckdb)
set_target_properties(ssb_duckdb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/ssb)

file(COPY benchmark/ssb/sql DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/ssb)

#----------------------------------------------
#   build Blob executables.
#----------------------------------------------

#       add_executable(blob_sqlite3 ${BLOB_SQLITE_MAIN_FILE})
#       target_include_directories(blob_sqlite3 PRIVATE src src/systems/sqlite)
#       target_link_libraries(blob_sqlite3 cxxopts dbbench_core sqlite3 sqlite3cpp)
#       set_target_properties(
#               blob_sqlite3
#               PROPERTIES
#               RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/blob
#       )

add_executable(blob_duckdb ${BLOB_DUCKDB_MAIN_FILE})
target_include_directories(blob_duckdb PRIVATE src)
target_link_libraries(blob_duckdb cxxopts dbbench_core duckdb)
set_target_properties(blob_duckdb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/blob)

# Scripts.
configure_file(benchmark/scripts/duckdb_ssb.sh ${CMAKE_CURRENT_BINARY_DIR}/ssb/sqlite_ssb.sh COPYONLY)
configure_file(benchmark/scripts/sqlite_ssb.sh ${CMAKE_CURRENT_BINARY_DIR}/ssb/duckdb_ssb.sh COPYONLY)
configure_file(benchmark/scripts/sqlite_tatp.sh ${CMAKE_CURRENT_BINARY_DIR}/tatp/sqlite_tatp.sh COPYONLY)
configure_file(benchmark/scripts/duckdb_tatp.sh ${CMAKE_CURRENT_BINARY_DIR}/tatp/duckdb_tatp.sh COPYONLY)
configure_file(benchmark/scripts/sqlite_blob.sh ${CMAKE_CURRENT_BINARY_DIR}/blob/sqlite_blob.sh COPYONLY)
configure_file(benchmark/scripts/duckdb_blob.sh ${CMAKE_CURRENT_BINARY_DIR}/blob/duckdb_blob.sh COPYONLY)
configure_file(benchmark/scripts/all.sh ${CMAKE_CURRENT_BINARY_DIR}/all.sh COPYONLY)
