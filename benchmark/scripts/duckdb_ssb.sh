#!/bin/bash

for sf in 1 2 5; do

  printf "Generating data...\n"
  rm -f ./*.tbl
  ./dbgen -s "$sf"

  printf "Loading data into DuckDB...\n"
  ./ssb_duckdb --load

  printf "Evaluating DuckDB...\n"
  for threads in 1 2 4; do
    command="./ssb_duckdb --run --threads=$threads --sf=$sf"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      printf "%s," "$trial"
      eval "$command"
    done
  done

  rm ssb.duckdb
done