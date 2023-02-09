#!/bin/bash

printf "______________________________________________________\n\t\t\tNew test run\n______________________________________________________\n" >> ../results/master_results.csv

chmod u+x ssb/ssb.sh
chmod u+x tatp/tatp.sh
chmod u+x blob/blob.sh
(
  cd ssb || exit
  ./ssb.sh
)

(
  cd tatp || exit
  ./tatp.sh
)

(
  cd blob || exit
  ./blob.sh
)