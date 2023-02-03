set(BENCHMARK_HELPER
    # ${CMAKE_SOURCE_DIR}/benchmark/bench.h
    # ${CMAKE_SOURCE_DIR}/benchmark/benchmark.c
    # ${CMAKE_SOURCE_DIR}/benchmark/histogram.c
    # ${CMAKE_SOURCE_DIR}/benchmark/random.c
    # ${CMAKE_SOURCE_DIR}/benchmark/raw.c
    # ${CMAKE_SOURCE_DIR}/benchmark/readfile.hpp
    # ${CMAKE_SOURCE_DIR}/benchmark/sqlite_helper.h
)


set(TATP_SQLITE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/tatp/sqlite_tatp.cpp)
set(TATP_MSC_DENSE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/tatp/msc_dense_tatp.cpp)
set(TATP_MSC_LARGE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/tatp/msc_large_tatp.cpp)
set(TATP_DUCKDB_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/tatp/tatp_duckdb.cpp)

set(SSB_SQLITE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/ssb/ssb_sqlite3.cpp)
set(SSB_MSC_DENSE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/ssb/ssb_msc_dense.cpp)
set(SSB_MSC_LARGE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/ssb/ssb_msc_large.cpp)
set(SSB_DUCKDB_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/ssb/ssb_duckdb.cpp)


set(BLOB_SQLITE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/blob/blob_sqlite3.cpp)
set(BLOB_MSC_DENSE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/blob/blob_msc_dense.cpp)
set(BLOB_MSC_LARGE_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/blob/blob_msc_large.cpp)
set(BLOB_DUCKDB_MAIN_FILE  ${CMAKE_SOURCE_DIR}/benchmark/blob/blob_duckdb.cpp)

