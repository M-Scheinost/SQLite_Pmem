# TODOS
- change benchmark to tatp, tpc-e doesn't make any sense as it is a client server OLTP benchmark and i don't have a client server relationship here
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

# sources
- sqlite3 from sqlite.org
- TATP from https://tatpbenchmark.sourceforge.net/
- benchmark setup from: https://github.com/UWHustle/sqlite-past-present-future


Author
=============
[Manuel Scheinost](https://github.com/M-Scheinost)