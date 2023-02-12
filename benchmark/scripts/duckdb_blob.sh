
#!/bin/bash
memlimit="0"
for sf in 100000 1000000 10000000; do
  printf "*** Blob benchmark (scale factor %s) ***\n" "$sf"
  path="/mnt/pmem0/scheinost/benchmark.db"
  [ ! -e $path ] || rm $path*

 for pm in "PMem" "unix"; do
    ./blob_sqlite3 --load --size=$sf --pmem=$pm --path=$path
    for mix in "0.9" "0.5" "0.1"; do
      command="./blob_sqlite3 --run --size=$sf --mix=$mix --path=$path --pmem=$pm --cache_size=$memlimit"
      for trial in {1..3}; do
        eval "$command"
      done
    done
    done
    rm $path*
done