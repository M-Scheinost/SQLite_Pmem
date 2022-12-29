# TODOS
- change the benchmark to tpc-e
    - added tpc-e folders 
- dynamic file_sizes similar to vector or other strategy
    - extending is now possible both for shm and normal, but its not tested

# How to build
I made a cmake file so do the magic:
```
mkdir build
cd build
cmake ..
make
```
The cmake supports Debug and Release mode, so set the -CMAKE_BUILD_TYPE accordingly.

# different programs which are implemented
- __shell__: just a plain unedited sqlite shell
- __test__: a program i use to test my pmem_vfs
- __bench__: a program to run the benchmark i found on Github. This will soon be TPC-E

# How to create the csvs
- generate the data using `EGenLoader`
- use the `extract.py` to transform the txt files to csvs


# How to TPC-E

## EGenLoader
`./EGenLoader -i ../flat_in -o ../flat_out -c 2000 -t 2000 -f 1000 -w 50`
```
EGen v1.14.0
Usage:
EGenLoader [options] 

 Where
  Option                       Default     Description
   -b number                   1           Beginning customer ordinal position
   -c number                   5000        Number of customers (for this instance)
   -t number                   5000        Number of customers (total in the database)
   -f number                   500         Scale factor (customers per 1 tpsE) (a larger number generates less data)
   -w number                   300          Number of Workdays (8-hour days) of 
                                           initial trades to populate
   -i dir                      flat_in/    Directory for input files
   -l [FLAT|ODBC|CUSTOM|NULL]  FLAT        Type of load
   -m [APPEND|OVERWRITE]       OVERWRITE   Flat File output mode
   -o dir                      flat_out/   Directory for output files

   -x                          -x          Generate all tables
   -xf                                     Generate all fixed-size tables
   -xd                                     Generate all scaling and growing tables
                                           (equivalent to -xs -xg)
   -xs                                     Generate scaling tables
                                           (except BROKER)
   -xg                                     Generate growing tables and BROKER
   -g                                      Disable caching when generating growing tables
```

# sources
- sqlite3 from sqlite.org
- the tpc-e setup from https://github.com/suhascv/SQLite_TPC-E

Author
=============
[Manuel Scheinost](https://github.com/M-Scheinost)