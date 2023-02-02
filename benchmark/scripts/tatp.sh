#!/bin/bash

for sf in 10000 100000 1000000 10000000; do
  #---------------------------------------------
  #       sqlite
  #---------------------------------------------
  for pm in "PMem" "unix"; do
    printf "*** TATP (scale factor $sf ) ***\n"
    path=""
    if [ "$pm" = "PMem" ]
    then
      path+="/mnt/pmem0/scheinost/benchmark.db"
    else
      path+="benchmark.db"
    fi
    [ ! -e $path ] || rm $path*

    printf "Loading data into SQLite3 Pmem=$pm\n"
    ./tatp_sqlite --load --records=$sf --path=$path --pmem=$pm

    printf "Evaluating Pmem=$pm\n"
      command="./tatp_sqlite --run --records=$sf --path=$path --pmem=$pm" 
      printf "%s\n" "$command"
      for trial in {1..3}; do
          printf "%s," "$trial"
          eval "$command"
      done
    rm $path*
  done
  
#---------------------------------------------
#       msc-dense
#---------------------------------------------
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

#---------------------------------------------
#       msc-large
#---------------------------------------------

  printf "*** TATP (scale factor $sf ) ***\n"
  path="/mnt/pmem0/scheinost/benchmark.db"
  pm="PMem"
  [ ! -e $path ] || rm $path*

  printf "Loading data into SQLite3 Pmem=$pm\n"
  ./tatp_msc_large --load --records=$sf --path=$path --pmem=$pm

  printf "Evaluating Pmem=$pm\n"
  command="./tatp_msc_large --run --records=$sf --path=$path --pmem=$pm" 
  printf "%s\n" "$command"
  for trial in {1..3}; do
    [ ! -e $path-shm ] || rm $path-shm
    printf "%s," "$trial"
    eval "$command"
  done
  rm $path*


#---------------------------------------------
#       duckdb
#---------------------------------------------
  printf "Loading data into DuckDB...\n"
    ./tatp_duckdb --load --records=$sf

  printf "Evaluating DuckDB...\n"
  #for memory_limit in "100MB" "200MB" "500MB" "1GB" "2GB" "5GB"; do
  command="./tatp_duckdb --run --records=$sf"
  printf "%s\n" "$command"
  for trial in {1..3}; do
    printf "%s,\n" "$trial"
    eval "$command"
  done

  rm tatp.duckdb
done