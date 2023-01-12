add_library(duckdb
        ${CMAKE_SOURCE_DIR}/duckdb/duckdb.cpp
        ${CMAKE_SOURCE_DIR}/duckdb/duckdb.hpp
)
target_link_libraries(duckdb Threads::Threads ${CMAKE_DL_LIBS})