#!/bin/bash

printf "#########################################\n\t\tnew test run\n#########################################\n" >> /home/scheinost/SQLite_Pmem/results.csv

for sf in 10000 100000 1000000 10000000; do
    printf "*** TATP (scale factor $sf ) ***\n"
    [ ! -e $path ] || rm $path*

    printf "Loading data into SQLite3 Pmem=$pm\n"
    ./tatp_bench --records=$sf
    rm $path*
    if [ "$pm" = "wal-only" ]
    then
      rm /mnt/pmem0/scheinost/database.db-wal
      rm /mnt/pmem0/scheinost/database.db-shm
    fi
  done
done