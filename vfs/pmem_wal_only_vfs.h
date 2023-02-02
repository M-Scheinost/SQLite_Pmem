#ifndef PMEM_VFS_WAL_ONLY_H
#define PMEM_VFS_WAL_ONLY_H
#include "../sqlite/sqlite/sqlite3.h"
#include <libpmem.h>
#include <libpmemlog.h>

#include <assert.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/time.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>

typedef u_int64_t u64;
typedef u_int32_t u32;
typedef u_int16_t u16;
typedef u_int8_t u8;

typedef int64_t i64;
typedef int32_t i32;
typedef int16_t i16;
typedef int8_t i8;


/*
** Size of the write buffer used by journal files in bytes.
*/
#ifndef PMEM_BUFFER_SIZE
# define PMEM_BUFFER_SIZE 2<<14
#endif

#ifndef GROW_FACTOR_FILE
# define GROW_FACTOR_FILE 2
#endif

#ifndef SHM_BASE_SIZE
/* shm must be at least 32kB large*/
#define SHM_BASE_SIZE ((off_t)(1 << 15))
#endif

/* This is 32kB of pmem in bytes*/
#ifndef PMEM_LEN
# define PMEM_LEN ((off_t)(1 << 13))
#endif

// 2^30 ~ 1GB
#ifndef PMEM_MAX_LEN
# define PMEM_MAX_LEN ((off_t)(1 << 30))
#endif

/*
** The maximum pathname length supported by this VFS.
*/
#define MAXPATHNAME 512

/*The only function visible from the outside*/
sqlite3_vfs *sqlite3_pmem_wal_only_vfs(void);

#endif // PMEM_VFS_WAL_ONLY_H