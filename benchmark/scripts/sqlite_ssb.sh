#!/bin/bash

for sf in 1 2 5; do
  printf "*** SSB (scale factor %s) ***\n" "$sf"

  printf "Generating data...\n"
  rm -f ./*.tbl
  ./dbgen -s "$sf"

  printf "Loading data into SQLite3...\n"
  ../sqlite3_shell ssb.sqlite <sql/init/sqlite3.sql

  printf "Evaluating SQLite3...\n"
  for bloom_filter in "false" "true"; do
    for cache_size in "-100000" "-200000" "-500000" "-1000000" "-2000000" "-5000000"; do
      command="./ssb_sqlite3 --bloom_filter=$bloom_filter --cache_size=$cache_size"
      printf "%s\n" "$command"
      printf "trial,Q1.1,Q1.2,Q1.3,Q2.1,Q2.2,Q2.3,Q3.1,Q3.2,Q3.3,Q3.4,Q4.1,Q4.2,Q4.3\n"
      for trial in {1..3}; do
        printf "%s," "$trial"
        eval "$command"
      done
    done
  done

  rm ssb.sqlite
done