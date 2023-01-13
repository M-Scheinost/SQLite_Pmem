
#!/bin/bash
for sf in 100000 1000000 10000000; do
  printf "Loading data into DuckDB...\n"
  ./blob_duckdb --load --size=$sf

  printf "Evaluating DuckDB...\n"
  for mix in "0.9" "0.5" "0.1"; do
    command="./blob_duckdb --run --size=$sf --mix=$mix"
    printf "%s\n" "$command"
    for trial in {1..3}; do
      printf "%s," "$trial"
      eval "$command"
    done
  done

  rm blob.duckdb
done