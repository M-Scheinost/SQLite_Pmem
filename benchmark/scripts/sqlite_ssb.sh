#!/bin/bash

for sf in 1 2 5; do
  for pm in "PMem" "unix"; do
  printf "*** SSB (scale factor %s) ***\n" "$sf"

  path=""
    if [ "$pm" = "true" ]
    then
      path+="/mnt/pmem0/scheinost/benchmark.db"
    else
      path+="benchmark.db"
    fi
    [ ! -e $path ] || rm $path*



  printf "Generating data...\n"
  rm -f ./*.tbl
  ./dbgen -s "$sf"

  printf "Loading data into SQLite3...\n"
  ../sqlite3_shell $path <sql/init/sqlite3.sql

  printf "Evaluating SQLite3...\n"
  for bloom_filter in "false" "true"; do
      command="./ssb_sqlite3 --bloom_filter=$bloom_filter --cache_size=$cache_size --sf=$sf --path=$path"
      printf "%s\n" "$command"
      for trial in {1..3}; do
        printf "%s," "$trial"
        eval "$command"
      done
    done
  done

  rm $path*
done