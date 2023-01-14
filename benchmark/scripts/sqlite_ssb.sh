#!/bin/bash

for sf in 1 2 5; do
  printf "Generating data...\n"
  rm -f ./*.tbl
  ./dbgen -s "$sf"

  for pm in "unix" "PMem"; do
  printf "*** SSB (scale factor %s) ***\n" "$sf"

  path=""
    if [ "$pm" = "PMem" ]
    then
      path+="/mnt/pmem0/scheinost/benchmark.db"
    else
      path+="benchmark.db"
    fi
    [ ! -e $path ] || rm $path*

  printf "Loading data into SQLite3...\n"
  ../sqlite3_shell $path <sql/init/sqlite3.sql

  printf "Evaluating SQLite3...\n"
  for bloom_filter in "false" "true"; do
      command="./ssb_sqlite3 --bloom_filter=$bloom_filter --sf=$sf --path=$path --pmem=$pm"
      printf "%s\n" "$command"
      for trial in {1..3}; do
        printf "%s," "$trial"
        eval "$command"
      done
    done
    rm $path*
  done  
done