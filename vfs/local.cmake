set(VFS_FILES
    ${CMAKE_SOURCE_DIR}/vfs/pmem_vfs.h
    ${CMAKE_SOURCE_DIR}/vfs/pmem_vfs.c
)

add_library(vfs ${VFS_FILES})