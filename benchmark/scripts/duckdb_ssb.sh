#!/bin/bash

for sf in 1 2 5; do
  
  printf "Generating data...\n"
  rm -f ./*.tbl
  ./dbgen -s "$sf"

  path="/mnt/pmem0/scheinost/benchmark.db"  
  printf "Loading data into DuckDB...\n"
  rm $path*
  ./ssb_duckdb --load --path=$path
  printf "Evaluating DuckDB...\n"
  threads="1"
    command="./ssb_duckdb --run --threads=$threads --sf=$sf --path=$path --memory_limit=200MB"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      printf "%s," "$trial"
      eval "$command"
    done
    printf "\n"

  rm $path*
done