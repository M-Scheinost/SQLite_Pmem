set(VFS_FILES
    ${CMAKE_SOURCE_DIR}/vfs/pmem_vfs.h
    ${CMAKE_SOURCE_DIR}/vfs/pmem_vfs.c
    ${CMAKE_SOURCE_DIR}/vfs/pmem_wal_only_vfs.c
    ${CMAKE_SOURCE_DIR}/vfs/pmem_wal_only_vfs.h
    ${CMAKE_SOURCE_DIR}/vfs/test_demovfs.c
    ${CMAKE_SOURCE_DIR}/vfs/test_demovfs.h
)

add_library(vfs ${VFS_FILES})