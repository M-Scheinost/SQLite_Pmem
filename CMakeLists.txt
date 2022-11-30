#--------------------------------------------
#   for sqlite build see:
#   source: https://github.com/snikulov/sqlite.cmake.build/blob/master/CMakeLists.txt
#--------------------------------------------
#--------------------------------------------
# SQLite build script for amalgamation
cmake_minimum_required(VERSION 3.10)
project(PMEM_SQLITE C)

find_package(Threads REQUIRED)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS -pthread)

add_library(PMEM_SQLITE benchmark/main.c sqlite/sqlite3.h vfs/pmem_vfs.c sqlite/sqlite3.c benchmark/bench.h )
target_link_libraries(PMEM_SQLITE ${CMAKE_DL_LIBS} pmem pmemblk pmemlog Threads::Threads)