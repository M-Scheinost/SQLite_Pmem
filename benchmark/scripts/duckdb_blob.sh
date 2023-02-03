
#!/bin/bash
for sf in 100000 1000000 10000000; do
  printf "*** Blob benchmark (scale factor %s) ***\n" "$sf"
  pm="PMem"
  path="/mnt/pmem0/scheinost/benchmark.db"
  [ ! -e $path ] || rm $path*

  printf "Loading data into SQLite3...\n"
  ./blob_msc_dense --load --size=$sf --pmem=$pm --path=$path

  printf "Evaluating SQLite3...\n"
  for mix in "0.9" "0.5" "0.1"; do
    command="./blob_msc_dense --run --size=$sf --mix=$mix --path=$path --pmem=$pm"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      [ ! -e $path-shm ] || rm $path-*
      printf "%s," "$trial"
      eval "$command"
    done
    printf "\n"
  done
  rm $path*
done