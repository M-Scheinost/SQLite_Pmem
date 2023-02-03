#!/bin/bash

for sf in 1 2 5; do
  printf "Generating data...\n"
  rm -f ./*.tbl
  ./dbgen -s "$sf"

  #---------------------------------------------
  #       sqlite
  #---------------------------------------------
  for pm in "PMem" "unix"; do
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
      printf "\n"
    done
    rm $path*
  done

#---------------------------------------------
#       msc-dense
#---------------------------------------------
  printf "*** SSB (scale factor %s) ***\n" "$sf"
  pm="PMem"
  path="/mnt/pmem0/scheinost/benchmark.db"  
  [ ! -e $path ] || rm $path*

  printf "Loading data into SQLite3...\n"
  ../sqlite3_shell $path <sql/init/sqlite3.sql

  printf "Evaluating SQLite3...\n"
  for bloom_filter in "false" "true"; do
    command="./ssb_msc_dense --bloom_filter=$bloom_filter --sf=$sf --path=$path --pmem=$pm"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      [ ! -e $path-shm ] || rm $path-shm
      printf "%s," "$trial"
      eval "$command"
    done
    printf "\n"
  done
  rm $path*

#---------------------------------------------
#       msc-large
#---------------------------------------------
  printf "*** SSB (scale factor %s) ***\n" "$sf"
  pm="PMem"
  path="/mnt/pmem0/scheinost/benchmark.db"  
  [ ! -e $path ] || rm $path*

  printf "Loading data into SQLite3...\n"
  ../sqlite3_shell $path <sql/init/sqlite3.sql

  printf "Evaluating SQLite3...\n"
  for bloom_filter in "false" "true"; do
    command="./ssb_msc_large --bloom_filter=$bloom_filter --sf=$sf --path=$path --pmem=$pm"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      [ ! -e $path-shm ] || rm $path-shm
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
  ./ssb_duckdb --load --path=$path

  printf "Evaluating DuckDB...\n"
  for threads in 1 2 4; do
    command="./ssb_duckdb --run --threads=$threads --sf=$sf --path=$path"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      printf "%s," "$trial"
      eval "$command"
    done
    printf "\n"
  done

  rm $path*
done