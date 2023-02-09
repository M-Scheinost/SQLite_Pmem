#!/bin/bash
memlimit="-48828"
for sf in 100000 1000000 10000000; do

  #---------------------------------------------
  #       sqlite
  #---------------------------------------------
  for pm in "PMem" "unix"; do
    printf "*** Blob benchmark (scale factor %s) ***\n" "$sf"

    path="/mnt/pmem0/scheinost/benchmark.db"
    [ ! -e $path ] || rm $path*

    printf "Loading data into SQLite3...\n"
    ./blob_sqlite3 --load --size=$sf --pmem=$pm --path=$path

    printf "Evaluating SQLite3...\n"
    for mix in "0.9" "0.5" "0.1"; do
      command="./blob_sqlite3 --run --size=$sf --mix=$mix --path=$path --pmem=$pm --cach_size=$memlimit"
      printf "%s\n" "$command"
      for trial in {1..3}; do
        printf "%s," "$trial"
        eval "$command"
      done
      printf "\n"
    done
    rm $path*
  done

  #---------------------------------------------
  #       msc-dense
  #---------------------------------------------
  printf "*** Blob benchmark (scale factor %s) ***\n" "$sf"
  pm="PMem"
  path="/mnt/pmem0/scheinost/benchmark.db"
  [ ! -e $path ] || rm $path*

  printf "Loading data into SQLite3...\n"
  ./blob_msc_dense --load --size=$sf --pmem=$pm --path=$path

  printf "Evaluating SQLite3...\n"
  for mix in "0.9" "0.5" "0.1"; do
    command="./blob_msc_dense --run --size=$sf --mix=$mix --path=$path --pmem=$pm --cach_size=$memlimit"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      [ ! -e $path-shm ] || rm $path-*
      printf "%s," "$trial"
      eval "$command"
    done
    printf "\n"
  done
  rm $path*
#---------------------------------------------
#       msc-large
#---------------------------------------------
  printf "*** Blob benchmark (scale factor %s) ***\n" "$sf"
  pm="PMem"
  path="/mnt/pmem0/scheinost/benchmark.db"
  [ ! -e $path ] || rm $path*

  printf "Loading data into SQLite3...\n"
  ./blob_msc_large --load --size=$sf --pmem=$pm --path=$path

  printf "Evaluating SQLite3...\n"
  for mix in "0.9" "0.5" "0.1"; do
    command="./blob_msc_large --run --size=$sf --mix=$mix --path=$path --pmem=$pm --cach_size=$memlimit"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      [ ! -e $path-shm ] || rm $path-*
      printf "%s," "$trial"
      eval "$command"
    done
    printf "\n"
  done
  rm $path*
#---------------------------------------------
#       duckdb
#---------------------------------------------
  path="/mnt/pmem0/scheinost/benchmark.db"
  printf "Loading data into DuckDB...\n"
  ./blob_duckdb --load --size=$sf --path=$path

  printf "Evaluating DuckDB...\n"
  for mix in "0.9" "0.5" "0.1"; do
    command="./blob_duckdb --run --size=$sf --mix=$mix --path=$path --memory_limit=200MB"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      printf "%s," "$trial"
      eval "$command"
    done
    printf "\n"
  done
  rm $path*

done