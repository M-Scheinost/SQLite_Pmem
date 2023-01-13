#!/bin/bash

for sf in 10000 100000 1000000; do

  printf "Loading data into DuckDB...\n"
    ./tatp_duckdb --load --records=$sf

  printf "Evaluating DuckDB...\n"
  #for memory_limit in "100MB" "200MB" "500MB" "1GB" "2GB" "5GB"; do
  command="./tatp_duckdb --run --records=$sf"
  printf "%s\n" "$command"
  for trial in {1..3}; do
    printf "%s," "$trial"
    eval "$command"
  done

  rm tatp.duckdb
done