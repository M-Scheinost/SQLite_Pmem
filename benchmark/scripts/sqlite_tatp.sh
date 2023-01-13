
# printf "n______________________________________________________\n\t\t\tSQLITE_TATP_BENCHMARK\n______________________________________________________\n" >> ../../results/master_results.csv

for sf in 10000 100000 1000000 10000000; do
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
    if [ "$pm" = "wal-only" ]
    then
      rm /mnt/pmem0/scheinost/database.db-wal
      rm /mnt/pmem0/scheinost/database.db-shm
    fi
  done
done