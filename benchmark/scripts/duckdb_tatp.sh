#!/bin/bash

for sf in 10000 100000 1000000 10000000; do
  pm="PMem"
    printf "*** TATP (scale factor $sf ) ***\n"
    path="/mnt/pmem0/scheinost/benchmark.db"
    [ ! -e $path ] || rm $path*

    printf "Loading data into SQLite3 Pmem=$pm\n"
    ./tatp_sqlite --load --records=$sf --path=$path --pmem=$pm

    printf "Evaluating Pmem=$pm\n"
      command="./tatp_sqlite --run --records=$sf --path=$path --pmem=$pm --cache_size=-1049000" 
      printf "%s\n" "$command"
      for trial in {1..3}; do
          printf "%s," "$trial"
          eval "$command"
      done
    rm $path*
done