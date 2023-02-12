#!/bin/bash
memlimit="0"
path="/mnt/pmem0/scheinost/benchmark.db"
[ ! -e $path ] || rm $path*

for sf in 10000 100000 1000000 10000000; do
for trial in {1..3}; do
#---------------------------------------------
#       sqlite
#---------------------------------------------
  for pm in "PMem" "unix"; do
    ./tatp_sqlite --load --records=$sf --path=$path --pmem=$pm --cache_size=$memlimit
    ./tatp_sqlite --run --records=$sf --path=$path --pmem=$pm --cache_size=$memlimit
    rm $path*
  done
  
#---------------------------------------------
#       msc-dense
#---------------------------------------------

  pm="PMem"
  ./tatp_msc_dense --load --records=$sf --path=$path --pmem=$pm --cache_size=$memlimit
  [ ! -e $path-shm ] || rm $path-shm
  ./tatp_msc_dense --run --records=$sf --path=$path --pmem=$pm --cache_size=$memlimit
  rm $path*

#---------------------------------------------
#       msc-large
#---------------------------------------------
  pm="PMem"
  ./tatp_msc_large --load --records=$sf --path=$path --pmem=$pm --cache_size=$memlimit
  [ ! -e $path-shm ] || rm $path-shm
  ./tatp_msc_large --run --records=$sf --path=$path --pmem=$pm --cache_size=$memlimit
  rm $path*

#---------------------------------------------
#       duckdb
#---------------------------------------------

  ./tatp_duckdb --load --records=$sf --path=$path --memory_limit=200MB
  ./tatp_duckdb --run --records=$sf --path=$path --memory_limit=200MB
  rm $path*
  done
done