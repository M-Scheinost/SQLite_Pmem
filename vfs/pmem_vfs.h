#pragma once
#include "../sqlite/sqlite3.h"
#include <libpmem.h>
#include <libpmemlog.h>

#include <assert.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/param.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>

/*
** Size of the write buffer used by journal files in bytes.
*/
#ifndef SQLITE_DEMOVFS_BUFFERSZ
# define SQLITE_DEMOVFS_BUFFERSZ 8192
#endif

#define WAL_MODE 0

/* This is 2^30 of pmem in bytes*/
#ifndef PMEM_LEN
# define PMEM_LEN ((off_t)(1 << 30))
#endif

/*
** The maximum pathname length supported by this VFS.
*/
#define MAXPATHNAME 512

/*The only function visible from the outside*/
sqlite3_vfs *sqlite3_pmem_vfs(void);