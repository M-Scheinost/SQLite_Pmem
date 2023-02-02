#!/bin/bash

printf "______________________________________________________\n\t\t\tNew test run\n______________________________________________________\n" >> ../results/master_results.csv

chmod u+x ssb/ssb.sh
chmod u+x tatp/tatp.sh
chmod u+x blob/blob.sh
# chmod u+x tatp/duckdb_tatp.sh
# chmod u+x ssb/duckdb_ssb.sh
# chmod u+x blob/duckdb_blob.sh

(
  cd ssb || exit
  ./ssb.sh
  # ./duckdb_ssb.sh
)

(
  cd tatp || exit
  ./tatp.sh
  # ./duckdb_tatp.sh
)

(
  cd blob || exit
  ./blob.sh
  # ./duckdb_blob.sh
)