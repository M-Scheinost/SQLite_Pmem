#!/bin/bash
printf "______________________________________________________\n\t\t\tUnix cache=1gb\n______________________________________________________\n" >> ../results/master_results.csv

cd tatp

for sf in 10000 100000 1000000 10000000; do
  pm="unix"
  printf "*** TATP (scale factor $sf ) ***\n"
  path=""
  if [ "$pm" = "PMem" ]
  then
    path+="/mnt/pmem0/scheinost/benchmark.db"
  else
    path+="benchmark.db"
  fi
  path="/mnt/pmem0/scheinost/benchmark.db"
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

cd ../ssb

for sf in 1 2 5; do
  printf "Generating data...\n"
  rm -f ./*.tbl
  ./dbgen -s "$sf"

  #---------------------------------------------
  #       sqlite
  #---------------------------------------------
  pm="unix"
  printf "*** SSB (scale factor %s) ***\n" "$sf"
  path="/mnt/pmem0/scheinost/benchmark.db"
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


cd ../blob

for sf in 100000 1000000 10000000; do
   pm="unix"
    printf "*** Blob benchmark (scale factor %s) ***\n" "$sf"

    path="/mnt/pmem0/scheinost/benchmark.db"
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
      printf "\n"
    done
    rm $path*
done