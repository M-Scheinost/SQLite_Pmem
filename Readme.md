# TODOS
- create shm methods in pmem_vfs.c. they handle the db-shm file which is needed in for pmem

# How to build
I made a cmake file so do the magic:
```
mkdir build
cd build
cmake ..
make
```
The cmake supports Debug and Release mode, so set the -CMAKE_BUILD_TYPE accordingly.