#!/bin/bash
memlimit="-48828"
path="/mnt/pmem0/scheinost/benchmark.db"
[ ! -e $path ] || rm $path*
for sf in 100000 1000000 10000000; do

  #---------------------------------------------
  #       sqlite
  #---------------------------------------------
  for pm in "PMem" "unix"; do
    ./blob_sqlite3 --load --size=$sf --pmem=$pm --path=$path
    for mix in "0.9" "0.5" "0.1"; do
      command="./blob_sqlite3 --run --size=$sf --mix=$mix --path=$path --pmem=$pm --cache_size=$memlimit"
      for trial in {1..3}; do
        eval "$command"
      done
    done
    rm $path*
  done

  #---------------------------------------------
  #       msc-dense
  #---------------------------------------------
  pm="PMem"
  ./blob_msc_dense --load --size=$sf --pmem=$pm --path=$path
  for mix in "0.9" "0.5" "0.1"; do
    command="./blob_msc_dense --run --size=$sf --mix=$mix --path=$path --pmem=$pm --cache_size=$memlimit"
    for trial in {1..3}; do
      [ ! -e $path-shm ] || rm $path-*
      eval "$command"
    done
  done
  rm $path*
#---------------------------------------------
#       msc-large
#---------------------------------------------
  pm="PMem"
  ./blob_msc_large --load --size=$sf --pmem=$pm --path=$path

  for mix in "0.9" "0.5" "0.1"; do
    command="./blob_msc_large --run --size=$sf --mix=$mix --path=$path --pmem=$pm --cache_size=$memlimit"
    for trial in {1..3}; do
      [ ! -e $path-shm ] || rm $path-*
      eval "$command"
    done
  done
  rm $path*
#---------------------------------------------
#       duckdb
#---------------------------------------------
  ./blob_duckdb --load --size=$sf --path=$path

  for mix in "0.9" "0.5" "0.1"; do
    command="./blob_duckdb --run --size=$sf --mix=$mix --path=$path --memory_limit=200MB"
    for trial in {1..3}; do
      eval "$command"
    done
  done
  rm $path*

done