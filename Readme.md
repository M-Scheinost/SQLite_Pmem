# TODOS
- create shm methods in pmem_vfs.c. they handle the db-shm file which is needed in for pmem
- change the benchmark to tpc-e

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



Author
=============
[Manuel Scheinost](https://github.com/M-Scheinost)