#!/bin/bash

for sf in 100000 1000000 10000000; do
  for pm in "PMem" "unix"; do
    printf "*** Blob benchmark (scale factor %s) ***\n" "$sf"

    path=""
    if [ "$pm" = "PMem" ]
    then
      path+="/mnt/pmem0/scheinost/benchmark.db"
    else
      path+="benchmark.db"
    fi
    [ ! -e $path ] || rm $path*


    printf "Loading data into SQLite3...\n"
    ./blob_sqlite3 --load --size=$sf --pmem=$pm --path=$path

    printf "Evaluating SQLite3...\n"
    for mix in "0.9" "0.5" "0.1"; do
      command="./blob_sqlite3 --run --size=$sf --mix=$mix --path=$path --pmem=$pm"
      printf "%s\n" "$command"
      for trial in {1..3}; do
        printf "%s," "$trial"
        eval "$command"
      done
    done

    rm $path*
  done
done