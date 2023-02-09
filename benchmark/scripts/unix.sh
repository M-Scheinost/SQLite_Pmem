#!/bin/bash

cd tatp
for sync in "NORMAL" "FULL" ; do
  for cache in "-1049000" "0"; do
printf "______________________________________________________\n\t\t\tUnix cache=$cache\tsync=$sync\n______________________________________________________\n" >> ../../results/master_results.csv
    for sf in 10000 100000 1000000 10000000; do
      path="/mnt/pmem0/scheinost/benchmark.db"
      [ ! -e $path ] || rm $path*
      pm="unix"

  ./tatp_sqlite --load --records=$sf --path=$path --pmem=$pm$sync$cache
    command="./tatp_sqlite --run --records=$sf --path=$path --pmem=$pm$sync$cache --cache_size=$cache --sync=$sync"
    for trial in {1..3}; do
        printf "%s," "$trial"
        eval "$command"
    done
  rm $path*
    done
    done
done