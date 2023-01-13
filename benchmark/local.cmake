set(Tatp_benchmark
    # ${CMAKE_SOURCE_DIR}/benchmark/bench.h
    # ${CMAKE_SOURCE_DIR}/benchmark/benchmark.c
    # ${CMAKE_SOURCE_DIR}/benchmark/histogram.c
    # ${CMAKE_SOURCE_DIR}/benchmark/random.c
    # ${CMAKE_SOURCE_DIR}/benchmark/raw.c
    # ${CMAKE_SOURCE_DIR}/benchmark/util.c
    # ${CMAKE_SOURCE_DIR}/benchmark/helper.hpp
    )


set(TATP_SQLITE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/tatp/sqlite_tatp.cpp)
 set(TATP_DUCKDB_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/tatp/tatp_duckdb.cpp)

set(SSB_SQLITE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/ssb/ssb_sqlite3.cpp)
set(SSB_DUCKDB_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/ssb/ssb_duckdb.cpp)


set(BLOB_SQLITE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/blob/blob_sqlite3.cpp)
set(BLOB_DUCKDB_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/blob/blob_duckdb.cpp)

