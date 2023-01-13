#!/bin/bash

chmod u+x ssb/sqlite_ssb.sh
chmod u+x ssb/duckdb_ssb.sh
chmod u+x tatp/sqlite_tatp.sh
chmod u+x tatp/duckdb_tatp.sh
chmod u+x blob/sqlite_blob.sh
chmod u+x blob/duckdb_blob.sh

(
  cd ssb || exit
#  ./sqlite_ssb.sh
  ./duckdb_ssb.sh
)

(
  cd tatp || exit
  ./sqlite_tatp.sh
  ./duckdb_tatp.sh
)

(
  cd blob || exit
#  ./sqlite_blob.sh
  ./duckdb_blob.sh
)