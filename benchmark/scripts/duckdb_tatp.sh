#!/bin/bash

for sf in 10000 100000 1000000 10000000; do
  printf "*** TATP (scale factor $sf ) ***\n"
  path="/mnt/pmem0/scheinost/benchmark.db"
  pm="PMem"

  [ ! -e $path ] || rm $path*

  printf "Loading data into SQLite3 Pmem=$pm\n"
  ./tatp_msc_dense --load --records=$sf --path=$path --pmem=$pm

  printf "Evaluating Pmem=$pm\n"
    command="./tatp_msc_dense --run --records=$sf --path=$path --pmem=$pm" 
    printf "%s\n" "$command"
    for trial in {1..3}; do
        [ ! -e $path-shm ] || rm $path-shm
        printf "%s," "$trial"
        eval "$command"
    done
  rm $path*
done