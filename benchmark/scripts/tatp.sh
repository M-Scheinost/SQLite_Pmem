#!/bin/bash


for sf in 10000 100000 1000000; do
  for pm in "true" "false"; do
    printf "*** TATP (scale factor $sf ) ***\n"
    path=""
    if [ "$pm" = "true" ]
    then
      path+="/mnt/pmem0/scheinost/benchmark.db"
    else
      path+="benchmark.db"
    fi

    printf "Loading data into SQLite3 Pmem=$pm\n"
    ./tatp_bench --load --records=$sf --path=$path --pmem=$pm

    printf "Evaluating Pmem=$pm\n"
    #for cache_size in "-100000" "-200000" "-500000" "-1000000" "-2000000" "-5000000"; do
      #command="./tatp_bench --run --records=$sf --journal_mode=$journal_mode --cache_size=$cache_size --path=$path --pmem=$pm"
      command="./tatp_bench --run --records=$sf --path=$path --pmem=$pm" 
      printf "%s\n" "$command"
      for trial in {1..3}; do
        #printf "%s," "$trial"
        eval "$command"
      done
    #done
  done
  rm $path
done