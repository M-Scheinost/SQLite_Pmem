#!/bin/bash

(
  cd ssb || exit
  ./sqlite_ssb.sh
  ./duckdb_ssb.sh
)

(
  cd tatp || exit
  ./sqlite_tatp.sh
  ./duckdb_tatp.sh
)

(
  cd blob || exit
  ./sqlite_blob.sh
  ./duckdb_blob.sh
)