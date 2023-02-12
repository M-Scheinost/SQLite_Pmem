#!/bin/bash
memlimit="-48828"
for sf in 10000 100000 1000000 10000000; do
   printf "*** TATP (scale factor $sf ) ***\n"
  path="/mnt/pmem0/scheinost/benchmark.db"
  pm="PMem"
  [ ! -e $path ] || rm $path*

  ./tatp_duckdb --load --records=$sf --path=$path --memory_limit=1GB
  for trial in {1..3}; do
  printf "eval\n"
#---------------------------------------------
#       sqlite
#---------------------------------------------
    ./tatp_duckdb --run --records=$sf --path=$path --memory_limit=1GB
  rm $path*
done