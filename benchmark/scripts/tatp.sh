#!/bin/bash


for sf in 10000 100000 500000 1000000 2000000; do
  for pm in "wal-only" "false" "wal-true"; do
    printf "*** TATP (scale factor $sf ) ***\n"
    path=""
    if [ "$pm" = "true" ]
    then
      path+="/mnt/pmem0/scheinost/benchmark.db"
    else
      path+="benchmark.db"
    fi
    [ ! -e $path ] || rm $path*

    printf "Loading data into SQLite3 Pmem=$pm\n"
    ./tatp_bench --load --records=$sf --path=$path --pmem=$pm

    printf "Evaluating Pmem=$pm\n"
    #for cache_size in "-100000" "-200000" "-500000" "-1000000" "-2000000" "-5000000"; do
      #command="./tatp_bench --run --records=$sf --journal_mode=$journal_mode --cache_size=$cache_size --path=$path --pmem=$pm"
      command="./tatp_bench --run --records=$sf --path=$path --pmem=$pm" 
      printf "%s\n" "$command"
      for trial in {1..3}; do
          printf "%s," "$trial"
          eval "$command"
      done
    #done
    rm $path*
    if [ "$pm" = "wal-only" ]
    then
      rm /mnt/pmem0/scheinost/database.db-wal
      rm /mnt/pmem0/scheinost/database.db-shm
    fi
  done
done