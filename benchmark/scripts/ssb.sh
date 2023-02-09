#!/bin/bash

memlimit="-48828"
path="/mnt/pmem0/scheinost/benchmark.db"
[ ! -e $path ] || rm $path*
for sf in 1 2 5; do
  printf "Generating data...\n"
  rm -f ./*.tbl
  ./dbgen -s "$sf"

  #---------------------------------------------
  #       sqlite
  #---------------------------------------------
  for pm in "PMem" "unix"; do

  ../sqlite3_shell $path <sql/init/sqlite3.sql

  for bloom_filter in "false" "true"; do
      command="./ssb_sqlite3 --bloom_filter=$bloom_filter --sf=$sf --path=$path --pmem=$pm --cache_size=$memlimit"
      for trial in {1..3}; do
        [ ! -e $path-shm ] || rm $path-*
        eval "$command"
      done
    done
    rm $path*
  done

#---------------------------------------------
#       msc-dense
#---------------------------------------------
  pm="PMem"
  ../sqlite3_shell $path <sql/init/sqlite3.sql

  for bloom_filter in "false" "true"; do
    command="./ssb_msc_dense --bloom_filter=$bloom_filter --sf=$sf --path=$path --pmem=$pm --cache_size=$memlimit"
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
  ../sqlite3_shell $path <sql/init/sqlite3.sql

  for bloom_filter in "false" "true"; do
    command="./ssb_msc_large --bloom_filter=$bloom_filter --sf=$sf --path=$path --pmem=$pm --cache_size=$memlimit"
    for trial in {1..3}; do
      [ ! -e $path-shm ] || rm $path-*
      eval "$command"
    done
  done
  rm $path*

#---------------------------------------------
#       duckdb
#---------------------------------------------
  ./ssb_duckdb --load --path=$path

  threads="1"
  command="./ssb_duckdb --run --threads=$threads --sf=$sf --path=$path --memory_limit=200MB"
  for trial in {1..3}; do
    eval "$command"
  done
  rm $path*
done