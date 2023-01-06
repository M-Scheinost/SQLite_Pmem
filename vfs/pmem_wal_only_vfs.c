/**
 * Author:   Manuel Scheinost
 * created: Nov 2022
 * 
*/


/*
** 2010 April 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file implements simple VFS implementation for persisten memory. It 
** omits complex features often not required. 
**
** OVERVIEW
**
**   The code in this file implements a SQLite VFS for persistent memory 
**   that can only be used on Linux operating systems. The following 
**   system calls are used:
**
**    File-system: access(), unlink(), getcwd()
**    File IO:     open(), read(), write(), fsync(), close(), fstat()
**    Other:       sleep(), usleep(), time()
**
**   The following VFS features are omitted:
**
**     1. File locking. The user must ensure that there is at most one
**        connection to each database when using this VFS. Multiple
**        connections to a single shared-cache count as a single connection
**        for the purposes of the previous statement.
**
**     2. The loading of dynamic extensions (shared libraries).
**
**     3. Temporary files. The user must configure SQLite to use in-memory
**        temp files when using this VFS. The easiest way to do this is to
**        compile with:
**
**          -DSQLITE_TEMP_STORE=3
**
**     4. File truncation. As of version 3.6.24, SQLite may run without
**        a working xTruncate() call, providing the user does not configure
**        SQLite to use "journal_mode=truncate", or use both
**        "journal_mode=persist" and ATTACHed databases.
**
** JOURNAL WRITE-BUFFERING
**
**   To commit a transaction to the database, SQLite first writes rollback
**   information into the journal file. This usually consists of 4 steps:
**
**     1. The rollback information is sequentially written into the journal
**        file, starting at the start of the file.
**     2. The journal file is synced to disk.
**     3. A modification is made to the first few bytes of the journal file.
**     4. The journal file is synced to disk again.
**
**   Most of the data is written in step 1 using a series of calls to the
**   VFS xWrite() method. The buffers passed to the xWrite() calls are of
**   various sizes. For example, as of version 3.6.24, when committing a 
**   transaction that modifies 3 pages of a database file that uses 4096 
**   byte pages residing on a media with 512 byte sectors, SQLite makes 
**   eleven calls to the xWrite() method to create the rollback journal, 
**   as follows:
**
**             Write offset | Bytes written
**             ----------------------------
**                        0            512
**                      512              4
**                      516           4096
**                     4612              4
**                     4616              4
**                     4620           4096
**                     8716              4
**                     8720              4
**                     8724           4096
**                    12820              4
**             ++++++++++++SYNC+++++++++++
**                        0             12
**             ++++++++++++SYNC+++++++++++
**
**   On many operating systems, this is an efficient way to write to a file.
**   However, on some embedded systems that do not cache writes in OS 
**   buffers it is much more efficient to write data in blocks that are
**   an integer multiple of the sector-size in size and aligned at the
**   start of a sector.
**
**   To work around this, the code in this file allocates a fixed size
**   buffer of SQLITE_DEMOVFS_BUFFERSZ using sqlite3_malloc() whenever a 
**   journal file is opened. It uses the buffer to coalesce sequential
**   writes into aligned SQLITE_DEMOVFS_BUFFERSZ blocks. When SQLite
**   invokes the xSync() method to sync the contents of the file to disk,
**   all accumulated data is written out, even if it does not constitute
**   a complete block. This means the actual IO to create the rollback 
**   journal for the example transaction above is this:
**
**             Write offset | Bytes written
**             ----------------------------
**                        0           8192
**                     8192           4632
**             ++++++++++++SYNC+++++++++++
**                        0             12
**             ++++++++++++SYNC+++++++++++
**
**   Much more efficient if the underlying OS is not caching write 
**   operations.
*/

#if !defined(SQLITE_TEST) || SQLITE_OS_UNIX

#include "pmem_vfs.h"
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "../sqlite/sqlite3.h"


/**
 * forward declerations
*/
static int pmem_delete_file(const char *zPath, int dirSync);


/*
** Different Unix systems declare open() in different ways.  Same use
** open(const char*,int,mode_t).  Others use open(const char*,int,...).
** The difference is important when using a pointer to the function.
**
** The safest way to deal with the problem is to always use this wrapper
** which always has the same well-defined interface.
*/
static int posixOpen(const char *zFile, int flags, int mode){
  return open(zFile, flags, mode);
}

/* Forward reference */
static int openDirectory(const char* c, int* x){
  return SQLITE_OK;
}

/*
** Return the system page size.
**
** This function should not be called directly by other code in this file. 
** Instead, it should be called via macro osGetpagesize().
*/
static int unixGetpagesize(void){
#if OS_VXWORKS
  return 1024;
#elif defined(_BSD_SOURCE)
  return getpagesize();
#else
  return (int)sysconf(_SC_PAGESIZE);
#endif
}


/*
** Many system calls are accessed through pointer-to-functions so that
** they may be overridden at runtime to facilitate fault injection during
** testing and sandboxing.  The following array holds the names and pointers
** to all overrideable system calls.
*/
static struct unix_syscall {
  const char *zName;            /* Name of the system call */
  sqlite3_syscall_ptr pCurrent; /* Current value of the system call */
  sqlite3_syscall_ptr pDefault; /* Default value */
} aSyscall[] = {
  { "open",         (sqlite3_syscall_ptr)posixOpen,  0  },
#define osOpen      ((int(*)(const char*,int,int))aSyscall[0].pCurrent)

  { "close",        (sqlite3_syscall_ptr)close,      0  },
#define osClose     ((int(*)(int))aSyscall[1].pCurrent)

  { "access",       (sqlite3_syscall_ptr)access,     0  },
#define osAccess    ((int(*)(const char*,int))aSyscall[2].pCurrent)

  { "getcwd",       (sqlite3_syscall_ptr)getcwd,     0  },
#define osGetcwd    ((char*(*)(char*,size_t))aSyscall[3].pCurrent)

  { "stat",         (sqlite3_syscall_ptr)stat,       0  },
#define osStat      ((int(*)(const char*,struct stat*))aSyscall[4].pCurrent)

/*
** The DJGPP compiler environment looks mostly like Unix, but it
** lacks the fcntl() system call.  So redefine fcntl() to be something
** that always succeeds.  This means that locking does not occur under
** DJGPP.  But it is DOS - what did you expect?
*/
#ifdef __DJGPP__
  { "fstat",        0,                 0  },
#define osFstat(a,b,c)    0
#else     
  { "fstat",        (sqlite3_syscall_ptr)fstat,      0  },
#define osFstat     ((int(*)(int,struct stat*))aSyscall[5].pCurrent)
#endif

  { "ftruncate",    (sqlite3_syscall_ptr)ftruncate,  0  },
#define osFtruncate ((int(*)(int,off_t))aSyscall[6].pCurrent)

  { "fcntl",        (sqlite3_syscall_ptr)fcntl,      0  },
#define osFcntl     ((int(*)(int,int,...))aSyscall[7].pCurrent)

  { "read",         (sqlite3_syscall_ptr)read,       0  },
#define osRead      ((ssize_t(*)(int,void*,size_t))aSyscall[8].pCurrent)

#if defined(USE_PREAD) || SQLITE_ENABLE_LOCKING_STYLE
  { "pread",        (sqlite3_syscall_ptr)pread,      0  },
#else
  { "pread",        (sqlite3_syscall_ptr)0,          0  },
#endif
#define osPread     ((ssize_t(*)(int,void*,size_t,off_t))aSyscall[9].pCurrent)

#if defined(USE_PREAD64)
  { "pread64",      (sqlite3_syscall_ptr)pread64,    0  },
#else
  { "pread64",      (sqlite3_syscall_ptr)0,          0  },
#endif
#define osPread64 ((ssize_t(*)(int,void*,size_t,off64_t))aSyscall[10].pCurrent)

  { "write",        (sqlite3_syscall_ptr)write,      0  },
#define osWrite     ((ssize_t(*)(int,const void*,size_t))aSyscall[11].pCurrent)

#if defined(USE_PREAD) || SQLITE_ENABLE_LOCKING_STYLE
  { "pwrite",       (sqlite3_syscall_ptr)pwrite,     0  },
#else
  { "pwrite",       (sqlite3_syscall_ptr)0,          0  },
#endif
#define osPwrite    ((ssize_t(*)(int,const void*,size_t,off_t))\
                    aSyscall[12].pCurrent)

#if defined(USE_PREAD64)
  { "pwrite64",     (sqlite3_syscall_ptr)pwrite64,   0  },
#else
  { "pwrite64",     (sqlite3_syscall_ptr)0,          0  },
#endif
#define osPwrite64  ((ssize_t(*)(int,const void*,size_t,off64_t))\
                    aSyscall[13].pCurrent)

  { "fchmod",       (sqlite3_syscall_ptr)fchmod,          0  },
#define osFchmod    ((int(*)(int,mode_t))aSyscall[14].pCurrent)

#if defined(HAVE_POSIX_FALLOCATE) && HAVE_POSIX_FALLOCATE
  { "fallocate",    (sqlite3_syscall_ptr)posix_fallocate,  0 },
#else
  { "fallocate",    (sqlite3_syscall_ptr)0,                0 },
#endif
#define osFallocate ((int(*)(int,off_t,off_t))aSyscall[15].pCurrent)

  { "unlink",       (sqlite3_syscall_ptr)unlink,           0 },
#define osUnlink    ((int(*)(const char*))aSyscall[16].pCurrent)

  { "openDirectory",    (sqlite3_syscall_ptr)openDirectory,      0 },
#define osOpenDirectory ((int(*)(const char*,int*))aSyscall[17].pCurrent)

  { "mkdir",        (sqlite3_syscall_ptr)mkdir,           0 },
#define osMkdir     ((int(*)(const char*,mode_t))aSyscall[18].pCurrent)

  { "rmdir",        (sqlite3_syscall_ptr)rmdir,           0 },
#define osRmdir     ((int(*)(const char*))aSyscall[19].pCurrent)

#if defined(HAVE_FCHOWN)
  { "fchown",       (sqlite3_syscall_ptr)fchown,          0 },
#else
  { "fchown",       (sqlite3_syscall_ptr)0,               0 },
#endif
#define osFchown    ((int(*)(int,uid_t,gid_t))aSyscall[20].pCurrent)

#if defined(HAVE_FCHOWN)
  { "geteuid",      (sqlite3_syscall_ptr)geteuid,         0 },
#else
  { "geteuid",      (sqlite3_syscall_ptr)0,               0 },
#endif
#define osGeteuid   ((uid_t(*)(void))aSyscall[21].pCurrent)

#if !defined(SQLITE_OMIT_WAL) || SQLITE_MAX_MMAP_SIZE>0
  { "mmap",         (sqlite3_syscall_ptr) mmap,            0 },
#else
  { "mmap",         (sqlite3_syscall_ptr)0,               0 },
#endif
#define osMmap ((void*(*)(void*,size_t,int,int,int,off_t))aSyscall[22].pCurrent)

#if !defined(SQLITE_OMIT_WAL) || SQLITE_MAX_MMAP_SIZE>0
  { "munmap",       (sqlite3_syscall_ptr) munmap,          0 },
#else
  { "munmap",       (sqlite3_syscall_ptr)0,               0 },
#endif
#define osMunmap ((int(*)(void*,size_t))aSyscall[23].pCurrent)

#if HAVE_MREMAP && (!defined(SQLITE_OMIT_WAL) || SQLITE_MAX_MMAP_SIZE>0)
  { "mremap",       (sqlite3_syscall_ptr)mremap,          0 },
#else
  { "mremap",       (sqlite3_syscall_ptr)0,               0 },
#endif
#define osMremap ((void*(*)(void*,size_t,size_t,int,...))aSyscall[24].pCurrent)

#if !defined(SQLITE_OMIT_WAL) || SQLITE_MAX_MMAP_SIZE>0
  { "getpagesize",  (sqlite3_syscall_ptr)unixGetpagesize, 0 },
#else
  { "getpagesize",  (sqlite3_syscall_ptr)0,               0 },
#endif
#define osGetpagesize ((int(*)(void))aSyscall[25].pCurrent)

#if defined(HAVE_READLINK)
  { "readlink",     (sqlite3_syscall_ptr)readlink,        0 },
#else
  { "readlink",     (sqlite3_syscall_ptr)0,               0 },
#endif
#define osReadlink ((ssize_t(*)(const char*,char*,size_t))aSyscall[26].pCurrent)

#if defined(HAVE_LSTAT)
  { "lstat",         (sqlite3_syscall_ptr)lstat,          0 },
#else
  { "lstat",         (sqlite3_syscall_ptr)0,              0 },
#endif
#define osLstat      ((int(*)(const char*,struct stat*))aSyscall[27].pCurrent)

#if defined(__linux__) && defined(SQLITE_ENABLE_BATCH_ATOMIC_WRITE)
# ifdef __ANDROID__
  { "ioctl", (sqlite3_syscall_ptr)(int(*)(int, int, ...))ioctl, 0 },
#define osIoctl ((int(*)(int,int,...))aSyscall[28].pCurrent)
# else
  { "ioctl",         (sqlite3_syscall_ptr)ioctl,          0 },
#define osIoctl ((int(*)(int,unsigned long,...))aSyscall[28].pCurrent)
# endif
#else
  { "ioctl",         (sqlite3_syscall_ptr)0,              0 },
#endif

}; /* End of the overrideable system calls */


/*
** When using this VFS, the sqlite3_file* handles that SQLite uses are
** actually pointers to instances of type Persistent_File.
*/
typedef struct Persistent_File Persistent_File;

struct Persistent_File {
 sqlite3_file base;                  /* Base class. Must be first. */

  const char* path;       /*path of the file*/
  int is_wal;             /*1 for wal file, 0 for database file*/
  int is_pmem;            /*1 if pmem, 0 otherwise*/
  size_t used_size;     /* the size which got used */
  size_t pmem_size;      /*The size of pmem-memory that was actually mapped, the pmem_file size*/
  char* pmem_file;        /*The entire pmem fiel represented as char array*/
  char* shm_file;     /* the wal-index file*/
  size_t shm_size;    /* size of the wal-index file*/
  size_t shm_used_size;    /* used mem of wal-index */
  int shm_is_pmem;
  int times_mapped; /* the number of times pmem_fetch was called*/
  char *shm_path;
};


int map_pmem_wal(Persistent_File* p, size_t new_size){
  //printf("map_pmem_wal%s\t%li\n",p->path, new_size);
  if(new_size == 0 ){
    struct stat st;
    int rc = stat(p->path, &st);
    if(rc){
      return SQLITE_IOERR;
    }
    new_size = st.st_size;
  }

  if(p->pmem_size == new_size){
    return SQLITE_OK;
  }
  // if(new_size > PMEM_MAX_LEN){
  //   new_size = PMEM_MAX_LEN;
  // }

  p->pmem_file = (char *)pmem_map_file(p->path, new_size, PMEM_FILE_CREATE, 0666, &p->pmem_size, &p->is_pmem);
  return SQLITE_OK;
}

void unmap_pmem_wal(Persistent_File* p){
  pmem_unmap(p->pmem_file, p->pmem_size);
  p->pmem_size = 0;
  p->used_size = 0;
  p->pmem_file = 0;
}


/*
*/
static int pmem_close(sqlite3_file *pFile){
  // // printf("close\n");
  Persistent_File *p = (Persistent_File*)pFile;
  
  unmap_pmem_wal(p);
  return SQLITE_OK;
}

/*
** Read data from a file.
*/
static int pmem_read(
  sqlite3_file *pFile,  /* the file*/
  void *buffer, /* the buffer to write the value to */
  int buffer_size, /* the size of the buffer */
  sqlite_int64 offset /*the offset to read */
){
  // // printf("read\n");
  Persistent_File *p = (Persistent_File*)pFile;

  if(offset + buffer_size <= p->used_size){
    memcpy(buffer,&((char*)p->pmem_file)[offset], buffer_size);
  }
  else{
    return SQLITE_IOERR_SHORT_READ;
  }
  return SQLITE_OK;
}


/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
static int pmem_write (
  sqlite3_file *pFile,
  const void *buffer,  
  int buffer_size, 
  sqlite_int64 offset
){
  Persistent_File *p = (Persistent_File*)pFile;
  //printf("try to write %i bytes at offset %lli to %s\n", buffer_size,offset,  p->path);
  assert ( pFile );
  assert( buffer_size > 0);

  while(p->pmem_size < offset + buffer_size){
    map_pmem_wal(p, p->pmem_size * GROW_FACTOR_FILE);
  }
      /* automatically flushes data to pmem no extra call needed*/
      //pmem_memcpy(p->pmem_file + offset, buffer, buffer_size, PMEM_F_MEM_NONTEMPORAL);
  
  if(p->is_pmem){
      //pmem_memcpy(&((char*)p->pmem_file)[offset],buffer, buffer_size, 0);
      memcpy(&((char*)p->pmem_file)[offset], buffer, buffer_size);
      pmem_persist(&((char*)p->pmem_file)[offset],buffer_size);
  }
  else{
      memcpy(&((char*)p->pmem_file)[offset], buffer, buffer_size);
      pmem_msync(&((char*)p->pmem_file)[offset], buffer_size);
  }
  

  if(offset + buffer_size > p->used_size){
    p->used_size = offset + buffer_size;
  }
  return SQLITE_OK; 
}

/*
** Truncate a file. This is a no-op for this VFS (see header comments at
** the top of the file).
*/
static int pmem_truncate(sqlite3_file *pFile, sqlite_int64 size){
  // // printf("truncate\n");
#if 0
  if( ftruncate(((DemoFile *)pFile)->fd, size) ) return SQLITE_IOERR_TRUNCATE;
#endif
  return SQLITE_OK;
}

/*
** Sync the contents of the file to the persistent media.
*/
static int pmem_sync(sqlite3_file *pFile, int flags){
  // // printf("pmem sync\n");
  Persistent_File *p = (Persistent_File*)pFile;

  if(p->is_pmem && !p->is_wal){
    return SQLITE_OK;
  }

  // if(p->is_pmem){
  //   pmem_persist(p->pmem_file, p->pmem_size);
  // }
  // else{
  //   pmem_msync(p->pmem_file, p->pmem_size);
  // }
  return SQLITE_OK;
}

/*
** Write the size of the file in bytes to *pSize.
*/
static int pmem_file_size(sqlite3_file *pFile, sqlite_int64 *pSize){
  // // printf("file size\n");
  Persistent_File *p = (Persistent_File*)pFile;
  *pSize = p->used_size;
  return SQLITE_OK;
}

/*
** Locking functions. The xLock() and xUnlock() methods are both no-ops.
** The xCheckReservedLock() always indicates that no other process holds
** a reserved lock on the database file. This ensures that if a hot-journal
** file is found in the file-system it is rolled back.
*/
inline static int pmem_lock(sqlite3_file *pFile, int eLock){
  // // printf("lock\n");
  return SQLITE_OK;
}
inline static int pmem_unlock(sqlite3_file *pFile, int eLock){
  // // printf("unlock\n");
  return SQLITE_OK;
}
inline static int pmem_check_reserved_lock(sqlite3_file *pFile, int *pResOut){
  // // printf("reserved lock\n");
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** No xFileControl() verbs are implemented by this VFS.
*/
static int pmem_file_control(sqlite3_file *pFile, int op, void *pArg){
  // // printf("file control\n");
  return SQLITE_NOTFOUND;
}

/*
** The xSectorSize() and xDeviceCharacteristics() methods. These two
** may return special values allowing SQLite to optimize file-system 
** access to some extent. But it is also safe to simply return 0.
*/
static int pmem_sector_size(sqlite3_file *pFile){
  /* 4096 is standard unix sector size*/
  return 4096;
}
static int pmem_device_characteristics(sqlite3_file *pFile){
  // // printf("device characteristics\n");
  return 0;
}


/**
 * opens the shm file
*/
static int pmem_open_shm(Persistent_File *p, size_t size){
  /* no tmp files allowd */
  if(p->path == 0 ){
    return SQLITE_IOERR;
  }

  const char *sp = "/mnt/pmem0/scheinost/database.db-shm";

  struct stat st;
  int rc = stat(sp, &st);
  if(rc){
    FILE *f = fopen(sp, "w");
    if(f == NULL){
      return SQLITE_IOERR;
    }
    fclose(f);
    size = SHM_BASE_SIZE;
  }
  else{
    if(size < st.st_size){
      size = st.st_size;
    }
  }
  // if(size > PMEM_MAX_LEN){
  //   size = PMEM_MAX_LEN;
  // }

  // 666 = rw-rw-rw
  if ((p->shm_file = (char *)pmem_map_file(sp, size, PMEM_FILE_CREATE,0666, &p->shm_size, &p->shm_is_pmem)) == NULL) {
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

/*
** This function is called to obtain a pointer to region iRegion of the 
** shared-memory associated with the database file fd. Shared-memory regions 
** are numbered starting from zero. Each shared-memory region is szRegion 
** bytes in size.
**
** If an error occurs, an error code is returned and *pp is set to NULL.
**
** Otherwise, if the bExtend parameter is 0 and the requested shared-memory
** region has not been allocated (by any client, including one running in a
** separate process), then *pp is set to NULL and SQLITE_OK returned. If 
** bExtend is non-zero and the requested shared-memory region has not yet 
** been allocated, it is allocated by this function.
**
** If the shared-memory region has already been allocated or is allocated by
** this call as described above, then it is mapped into this processes 
** address space (if it is not already), *pp is set to point to the mapped 
** memory and SQLITE_OK returned.
*/
static int pmem_map_shm(
  sqlite3_file *pFile,               /* Handle open on database file */
  int region_number,                    /* Region to retrieve */
  int region_size,                   /* Size of regions */
  int extend,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Mapped memory */
){
  Persistent_File *p = (Persistent_File*)pFile;
  //printf("Mapped-shm with: %i size %i number\n", region_size, region_number);

  /** 
   * if file was not allocated and extending is allowed, allocate the file
   * otherwise set pp to 0 and return ok
  */
  if(p->shm_file == 0){
    if(extend){
      pmem_open_shm(p,0);
    }
    else{
      *pp = 0;
    return SQLITE_OK;
    } 
  }
  int number_region = region_number +1;
  while(p->shm_size < region_size * number_region){
    //printf("extended shm to: %li\n", p->shm_size * GROW_FACTOR_FILE);
    pmem_open_shm(p, p->shm_size * GROW_FACTOR_FILE);
  }
  *pp = &((char*)p->shm_file)[region_number*region_size];

  if(region_size * number_region > p->used_size){
    p->shm_used_size = region_size * number_region;
  }

  //if(p->shm < region_size * region_number){
  //  p->used_size = offset + buffer_size;
  //}
  return SQLITE_OK;
}

/**
 * no locking in this vfs
*/
inline static int pmem_shm_lock(
  sqlite3_file *fd,          /* Database file holding the shared memory */
  int ofst,                  /* First lock to acquire or release */
  int n,                     /* Number of locks to acquire or release */
  int flags                  /* What to do with the lock */
){
  // // printf("shm-lock\n");
  return SQLITE_OK;
}

/*
** Implement a memory barrier or memory fence on shared memory.  
**
** All loads and stores begun before the barrier must complete before
** any load or store begun after the barrier.
*/
static void pmem_shm_barrier(
  sqlite3_file *pFile                /* Database file holding the shared memory */
){
  // printf("shm-barrier");
  Persistent_File *p = (Persistent_File*)pFile;

  if(p->shm_is_pmem){
    pmem_persist(p->shm_file, p->shm_size);
  }
  else{
    pmem_msync(p->pmem_file, p->pmem_size);
  }
}

/*
** Close a connection to shared-memory.  Delete the underlying 
** storage if deleteFlag is true.
**
** If there is no shared memory associated with the connection then this
** routine is a harmless no-op.
*/
static int pmem_shm_unmap(
  sqlite3_file *pFile,               /* The underlying database file */
  int deleteFlag                  /* Delete shared-memory if true */
){
  Persistent_File *p = (Persistent_File*)pFile;
  if(p->shm_file == 0){
    return SQLITE_OK;
  }
  pmem_unmap(p->shm_file, p->shm_size);
  p->shm_file = 0;
  if(deleteFlag){
    p->shm_size = 0;
    p->shm_used_size = 0;
    //pmem_delete_file(p->shm_path,1);
  }
  return SQLITE_OK; 
}

/*
** If possible, return a pointer to a mapping of file fd starting at offset
** iOff. The mapping must be valid for at least nAmt bytes.
**
** If such a pointer can be obtained, store it in *pp and return SQLITE_OK.
** Or, if one cannot but no error occurs, set *pp to 0 and return SQLITE_OK.
** Finally, if an error does occur, return an SQLite error code. The final
** value of *pp is undefined in this case.
**
** If this function does return a pointer, the caller must eventually 
** release the reference by calling unixUnfetch().
*/
static int pmem_fetch(sqlite3_file *fd, sqlite3_int64 offset, int amount, void **pp){
  return SQLITE_OK;
  printf("Fetch used\n");
   Persistent_File *p = (Persistent_File* )fd;
  
  *pp = 0;
  if (p->pmem_size > 0) {
      if (p->pmem_file == 0) {
          map_pmem_wal(p, 0);
      }
      if (offset + amount <= p->pmem_size) {
          *pp = (char *) p->pmem_file + offset;
          p->times_mapped++;
      }
  }
  return SQLITE_OK;
}

/*
** If the third argument is non-NULL, then this function releases a 
** reference obtained by an earlier call to unixFetch(). The second
** argument passed to this function must be the same as the corresponding
** argument that was passed to the unixFetch() invocation. 
**
** Or, if the third argument is NULL, then this function is being called 
** to inform the VFS layer that, according to POSIX, any existing mapping 
** may now be invalid and should be unmapped.
*/
static int pmem_unfetch(sqlite3_file *fd, sqlite3_int64 offset, void *p){
  return SQLITE_OK;
  printf("Unfetch used\n");
  Persistent_File* pf = (Persistent_File*) fd;
  if(p){
    pf->times_mapped--;
  }
  else{
    pmem_unmap(pf->pmem_file, pf->pmem_size);
    pf->pmem_size = 0;
    pf->pmem_file = 0;
    pf->used_size = 0;
  }
  

  return SQLITE_OK;
}




/*
** Open a file handle.
*/
static int pmem_open(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *file_path,              /* File to open, or 0 for a temp file */
  sqlite3_file *pFile,            /* Pointer to Persistent_file struct to populate */
  int flags,                      /* Input SQLITE_OPEN_XXX flags */
  int *pOutFlags                  /* Output SQLITE_OPEN_XXX flags (or NULL) */
){
  // printf("open database: %s\n", file_path);
  static const sqlite3_io_methods pmem_io = {
    3,                            /* iVersion */
    pmem_close,                    /* xClose */
    pmem_read,                     /* xRead */
    pmem_write,                    /* xWrite */
    pmem_truncate,                 /* xTruncate */
    pmem_sync,                     /* xSync */
    pmem_file_size,                 /* xFileSize */
    pmem_lock,                     /* xLock */
    pmem_unlock,                   /* xUnlock */
    pmem_check_reserved_lock,        /* xCheckReservedLock */
    pmem_file_control,              /* xFileControl */
    pmem_sector_size,               /* xSectorSize */
    pmem_device_characteristics,     /* xDeviceCharacteristics */
    pmem_map_shm,                     /* xShmMap */
    pmem_shm_lock,                /* xShmLock */
    pmem_shm_barrier,             /* xShmBarrier */
    pmem_shm_unmap,               /* xShmUnmap */
    pmem_fetch,                  /* xFetch */
    pmem_unfetch,                /* xUnfetch */
  };

  Persistent_File *p = (Persistent_File*)pFile; /* Populate this structure */

  /* no tmp files allowd */
  if( file_path == 0 ){
    return SQLITE_IOERR;
  }

  /* completly zeros p*/
  memset(p, 0, sizeof(Persistent_File));

  p->path = file_path;
  p->base.pMethods = &pmem_io;

// printf("OPEN_FLAGS:\t%i\n", flags);

  p->is_wal = flags & SQLITE_OPEN_WAL;
  if(p->is_wal){
    p->path = "/mnt/pmem0/scheinost/database.db-wal";
  }
  
  struct stat st;
  int rc = stat(p->path, &st);
  if(rc == 0){
    p->used_size = st.st_size;
    rc = map_pmem_wal(p, p->used_size);
  }
  else{
    p->used_size = 0;
    FILE *f = fopen(p->path, "w");
    if(f == NULL){
      return SQLITE_IOERR;
    }
    fclose(f);
    rc = map_pmem_wal(p, PMEM_LEN);
  }
  return rc;
}


/*
** Delete the file identified by argument zPath. If the dirSync parameter
** is non-zero, then ensure the file-system modification to delete the
** file has been synced to disk before returning.
*/
static int demoDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  int rc;                         /* Return code */

  rc = unlink(zPath);
  if( rc!=0 && errno==ENOENT ) return SQLITE_OK;

  if( rc==0 && dirSync ){
    int dfd;                      /* File descriptor open on directory */
    int i;                        /* Iterator variable */
    char *zSlash;
    char zDir[MAXPATHNAME+1];     /* Name of directory containing file zPath */

    /* Figure out the directory name from the path of the file deleted. */
    sqlite3_snprintf(MAXPATHNAME, zDir, "%s", zPath);
    zDir[MAXPATHNAME] = '\0';
    zSlash = strrchr(zDir,'/');
    if( zSlash ){
      /* Open a file-descriptor on the directory. Sync. Close. */
      zSlash[0] = 0;
      dfd = open(zDir, O_RDONLY, 0);
      if( dfd<0 ){
        rc = -1;
      }else{
        rc = fsync(dfd);
        close(dfd);
      }
    }
  }
  return (rc==0 ? SQLITE_OK : SQLITE_IOERR_DELETE);
}

static int pmem_delete_file(const char *zPath, int dirSync){
  int rc;                         /* Return code */

  rc = unlink(zPath);
  if( rc!=0 && errno==ENOENT ) return SQLITE_OK;

  if( rc==0 && dirSync ){
    int dfd;                      /* File descriptor open on directory */
    int i;                        /* Iterator variable */
    char *zSlash;
    char zDir[MAXPATHNAME+1];     /* Name of directory containing file zPath */

    /* Figure out the directory name from the path of the file deleted. */
    sqlite3_snprintf(MAXPATHNAME, zDir, "%s", zPath);
    zDir[MAXPATHNAME] = '\0';
    zSlash = strrchr(zDir,'/');
    if( zSlash ){
      /* Open a file-descriptor on the directory. Sync. Close. */
      zSlash[0] = 0;
      dfd = open(zDir, O_RDONLY, 0);
      if( dfd<0 ){
        rc = -1;
      }else{
        rc = fsync(dfd);
        close(dfd);
      }
    }
  }
  return (rc==0 ? SQLITE_OK : SQLITE_IOERR_DELETE);
}


/*
** Test the existence of or access permissions of file zPath. The
** test performed depends on the value of flags:
**
**     SQLITE_ACCESS_EXISTS: Return 1 if the file exists
**     SQLITE_ACCESS_READWRITE: Return 1 if the file is read and writable.
**     SQLITE_ACCESS_READONLY: Return 1 if the file is readable.
**
** Otherwise return 0.
*/
static int unixAccess(
  sqlite3_vfs *NotUsed,   /* The VFS containing this xAccess method */
  const char *zPath,      /* Path of the file to examine */
  int flags,              /* What do we want to learn about the zPath file? */
  int *pResOut            /* Write result boolean here */
){
  assert( pResOut!=0 );

  /* The spec says there are three possible values for flags.  But only
  ** two of them are actually used */
  assert( flags==SQLITE_ACCESS_EXISTS || flags==SQLITE_ACCESS_READWRITE );

  if( flags==SQLITE_ACCESS_EXISTS ){
    struct stat buf;
    *pResOut = 0==osStat(zPath, &buf) &&
                (!S_ISREG(buf.st_mode) || buf.st_size>0);
  }else{
    *pResOut = osAccess(zPath, W_OK|R_OK)==0;
  }
  return SQLITE_OK;
}

/*
** A pathname under construction
*/
typedef struct DbPath DbPath;
struct DbPath {
  int rc;           /* Non-zero following any error */
  int nSymlink;     /* Number of symlinks resolved */
  char *zOut;       /* Write the pathname here */
  int nOut;         /* Bytes of space available to zOut[] */
  int nUsed;        /* Bytes of zOut[] currently being used */
};

/* Forward reference */
static void appendAllPathElements(DbPath*,const char*);

/*
** Append a single path element to the DbPath under construction
*/
static void appendOnePathElement(
  DbPath *pPath,       /* Path under construction, to which to append zName */
  const char *zName,   /* Name to append to pPath.  Not zero-terminated */
  int nName            /* Number of significant bytes in zName */
){
  assert( nName>0 );
  assert( zName!=0 );
  if( zName[0]=='.' ){
    if( nName==1 ) return;
    if( zName[1]=='.' && nName==2 ){
      if( pPath->nUsed<=1 ){
        pPath->rc = SQLITE_ERROR;
        return;
      }
      assert( pPath->zOut[0]=='/' );
      while( pPath->zOut[--pPath->nUsed]!='/' ){}
      return;
    }
  }
  if( pPath->nUsed + nName + 2 >= pPath->nOut ){
    pPath->rc = SQLITE_ERROR;
    return;
  }
  pPath->zOut[pPath->nUsed++] = '/';
  memcpy(&pPath->zOut[pPath->nUsed], zName, nName);
  pPath->nUsed += nName;
#if defined(HAVE_READLINK) && defined(HAVE_LSTAT)
  if( pPath->rc==SQLITE_OK ){
    const char *zIn;
    struct stat buf;
    pPath->zOut[pPath->nUsed] = 0;
    zIn = pPath->zOut;
    if( osLstat(zIn, &buf)!=0 ){
      if( errno!=ENOENT ){
        pPath->rc = unixLogError(SQLITE_CANTOPEN_BKPT, "lstat", zIn);
      }
    }else if( S_ISLNK(buf.st_mode) ){
      ssize_t got;
      char zLnk[SQLITE_MAX_PATHLEN+2];
      if( pPath->nSymlink++ > SQLITE_MAX_SYMLINK ){
        pPath->rc = SQLITE_CANTOPEN_BKPT;
        return;
      }
      got = osReadlink(zIn, zLnk, sizeof(zLnk)-2);
      if( got<=0 || got>=(ssize_t)sizeof(zLnk)-2 ){
        pPath->rc = unixLogError(SQLITE_CANTOPEN_BKPT, "readlink", zIn);
        return;
      }
      zLnk[got] = 0;
      if( zLnk[0]=='/' ){
        pPath->nUsed = 0;
      }else{
        pPath->nUsed -= nName + 1;
      }
      appendAllPathElements(pPath, zLnk);
    }
  }
#endif
}

/*
** Append all path elements in zPath to the DbPath under construction.
*/
static void appendAllPathElements(
  DbPath *pPath,       /* Path under construction, to which to append zName */
  const char *zPath    /* Path to append to pPath.  Is zero-terminated */
){
  int i = 0;
  int j = 0;
  do{
    while( zPath[i] && zPath[i]!='/' ){ i++; }
    if( i>j ){
      appendOnePathElement(pPath, &zPath[j], i-j);
    }
    j = i+1;
  }while( zPath[i++] );
}

/*
** Turn a relative pathname into a full pathname. The relative path
** is stored as a nul-terminated string in the buffer pointed to by
** zPath. 
**
** zOut points to a buffer of at least sqlite3_vfs.mxPathname bytes 
** (in this case, MAX_PATHNAME bytes). The full-path is written to
** this buffer before returning.
*/
static int unixFullPathname(
  sqlite3_vfs *pVfs,            /* Pointer to vfs object */
  const char *zPath,            /* Possibly relative input path */
  int nOut,                     /* Size of output buffer in bytes */
  char *zOut                    /* Output buffer */
){
  DbPath path;
  path.rc = 0;
  path.nUsed = 0;
  path.nSymlink = 0;
  path.nOut = nOut;
  path.zOut = zOut;
  if( zPath[0]!='/' ){
    char zPwd[512+2];
    if( osGetcwd(zPwd, sizeof(zPwd)-2)==0 ){
      return SQLITE_ERROR;
    }
    appendAllPathElements(&path, zPwd);
  }
  appendAllPathElements(&path, zPath);
  zOut[path.nUsed] = 0;
  if( path.rc || path.nUsed<2 ) return SQLITE_ERROR;
  if( path.nSymlink ) return SQLITE_OK_SYMLINK;
  return SQLITE_OK;
}


/*
** The following four VFS methods:
**
**   xDlOpen
**   xDlError
**   xDlSym
**   xDlClose
**
** are supposed to implement the functionality needed by SQLite to load
** extensions compiled as shared objects. This simple VFS does not support
** this functionality, so the following functions are no-ops.
*/
inline static void *demoDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return 0;
}
inline static void demoDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
  zErrMsg[nByte-1] = '\0';
}
inline static void (*demoDlSym(sqlite3_vfs *pVfs, void *pH, const char *z))(void){
  return 0;
}
inline static void demoDlClose(sqlite3_vfs *pVfs, void *pHandle){
  return;
}


/*
** Parameter zByte points to a buffer nByte bytes in size. Populate this
** buffer with pseudo-random data.
*/
inline static int demoRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte){
  return SQLITE_OK;
}

/*
** Sleep for a little while.  Return the amount of time slept.
** The argument is the number of microseconds we want to sleep.
** The return value is the number of microseconds of sleep actually
** requested from the underlying operating system, a number which
** might be greater than or equal to the argument, but not less
** than the argument.
*/
static int unixSleep(sqlite3_vfs *NotUsed, int microseconds){
#if OS_VXWORKS
  struct timespec sp;

  sp.tv_sec = microseconds / 1000000;
  sp.tv_nsec = (microseconds % 1000000) * 1000;
  nanosleep(&sp, NULL);
  return microseconds;
#elif defined(HAVE_USLEEP) && HAVE_USLEEP
  if( microseconds>=1000000 ) sleep(microseconds/1000000);
  if( microseconds%1000000 ) usleep(microseconds%1000000);
  return microseconds;
#else
  int seconds = (microseconds+999999)/1000000;
  sleep(seconds);
  return seconds*1000000;
#endif
}

/*
** Set *pTime to the current UTC time expressed as a Julian day. Return
** SQLITE_OK if successful, or an error code otherwise.
**
**   http://en.wikipedia.org/wiki/Julian_day
**
** This implementation is not very good. The current time is rounded to
** an integer number of seconds. Also, assuming time_t is a signed 32-bit 
** value, it will stop working some time in the year 2038 AD (the so-called
** "year 2038" problem that afflicts systems that store time this way). 
*/
static int demoCurrentTime(sqlite3_vfs *pVfs, double *pTime){
  time_t t = time(0);
  *pTime = t/86400.0 + 2440587.5; 
  return SQLITE_OK;
}

/*
** The xGetLastError() method is designed to return a better
** low-level error message when operating-system problems come up
** during SQLite operation.  Only the integer return code is currently
** used.
*/
static int unixGetLastError(sqlite3_vfs *NotUsed, int NotUsed2, char *NotUsed3){
  return errno;
}

/*
** This is the xSetSystemCall() method of sqlite3_vfs for all of the
** "unix" VFSes.  Return SQLITE_OK opon successfully updating the
** system call pointer, or SQLITE_NOTFOUND if there is no configurable
** system call named zName.
*/
static int unixSetSystemCall(
  sqlite3_vfs *pNotUsed,        /* The VFS pointer.  Not used */
  const char *zName,            /* Name of system call to override */
  sqlite3_syscall_ptr pNewFunc  /* Pointer to new system call value */
){
  unsigned int i;
  int rc = SQLITE_NOTFOUND;

  if( zName==0 ){
    /* If no zName is given, restore all system calls to their default
    ** settings and return NULL
    */
    rc = SQLITE_OK;
    for(i=0; i<sizeof(aSyscall)/sizeof(aSyscall[0]); i++){
      if( aSyscall[i].pDefault ){
        aSyscall[i].pCurrent = aSyscall[i].pDefault;
      }
    }
  }else{
    /* If zName is specified, operate on only the one system call
    ** specified.
    */
    for(i=0; i<sizeof(aSyscall)/sizeof(aSyscall[0]); i++){
      if( strcmp(zName, aSyscall[i].zName)==0 ){
        if( aSyscall[i].pDefault==0 ){
          aSyscall[i].pDefault = aSyscall[i].pCurrent;
        }
        rc = SQLITE_OK;
        if( pNewFunc==0 ) pNewFunc = aSyscall[i].pDefault;
        aSyscall[i].pCurrent = pNewFunc;
        break;
      }
    }
  }
  return rc;
}

/*
** Return the value of a system call.  Return NULL if zName is not a
** recognized system call name.  NULL is also returned if the system call
** is currently undefined.
*/
static sqlite3_syscall_ptr unixGetSystemCall(
  sqlite3_vfs *pNotUsed,
  const char *zName
){
  unsigned int i;
  for(i=0; i<sizeof(aSyscall)/sizeof(aSyscall[0]); i++){
    if( strcmp(zName, aSyscall[i].zName)==0 ) return aSyscall[i].pCurrent;
  }
  return 0;
}

/*
** Return the name of the first system call after zName.  If zName==NULL
** then return the name of the first system call.  Return NULL if zName
** is the last system call or if zName is not the name of a valid
** system call.
*/
static const char *unixNextSystemCall(sqlite3_vfs *p, const char *zName){
  int i = -1;

  if( zName ){
    for(i=0; i<40-1; i++){
      if( strcmp(zName, aSyscall[i].zName)==0 ) break;
    }
  }
  for(i++; i<40; i++){
    if( aSyscall[i].pCurrent!=0 ) return aSyscall[i].zName;
  }
  return 0;
}
/*
** Find the current time (in Universal Coordinated Time).  Write into *piNow
** the current time and date as a Julian Day number times 86_400_000.  In
** other words, write into *piNow the number of milliseconds since the Julian
** epoch of noon in Greenwich on November 24, 4714 B.C according to the
** proleptic Gregorian calendar.
**
** On success, return SQLITE_OK.  Return SQLITE_ERROR if the time and date 
** cannot be found.
*/
static int unixCurrentTimeInt64(sqlite3_vfs *NotUsed, sqlite3_int64 *piNow){
  static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
  int rc = SQLITE_OK;
  struct timeval sNow;
  (void)gettimeofday(&sNow, 0);  /* Cannot fail given valid arguments */
  *piNow = unixEpoch + 1000*(sqlite3_int64)sNow.tv_sec + sNow.tv_usec/1000;
  return rc;
}

/*
** This function returns a pointer to the VFS implemented in this file.
** To make the VFS available to SQLite:
**
**   sqlite3_vfs_register(sqlite3_demovfs(), 0);
*/
sqlite3_vfs *sqlite3_pmem_wal_only_vfs(void){
  static sqlite3_vfs pmem_vfs = {
    3,                            /* iVersion */
    sizeof(Persistent_File),             /* szOsFile */
    MAXPATHNAME,                  /* mxPathname */
    0,                            /* pNext */
    "PMem_VFS_wal_only",                       /* zName */
    0,                            /* pAppData */
    pmem_open,                     /* xOpen */
    demoDelete,                   /* xDelete */
    unixAccess,                   /* xAccess */
    unixFullPathname,             /* xFullPathname */
    demoDlOpen,                   /* xDlOpen */
    demoDlError,                  /* xDlError */
    demoDlSym,                    /* xDlSym */
    demoDlClose,                  /* xDlClose */
    demoRandomness,               /* xRandomness */
    unixSleep,                    /* xSleep */
    demoCurrentTime,              /* xCurrentTime */
    unixGetLastError,     /* xGetLastError */
    unixCurrentTimeInt64, /* xCurrentTimeInt64 */
    unixSetSystemCall,    /* xSetSystemCall */
    unixGetSystemCall,    /* xGetSystemCall */
    unixNextSystemCall,   /* xNextSystemCall */
  };
  return &pmem_vfs;
}


#endif /* !defined(SQLITE_TEST) || SQLITE_OS_UNIX */


#ifdef SQLITE_TEST

#if defined(INCLUDE_SQLITE_TCL_H)
#  include "sqlite_tcl.h"
#else
#  include "tcl.h"
#  ifndef SQLITE_TCLAPI
#    define SQLITE_TCLAPI
#  endif
#endif

#if SQLITE_OS_UNIX
static int SQLITE_TCLAPI register_demovfs(
  ClientData clientData, /* Pointer to sqlite3_enable_XXX function */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  sqlite3_vfs_register(sqlite3_demovfs(), 1);
  return TCL_OK;
}
static int SQLITE_TCLAPI unregister_demovfs(
  ClientData clientData, /* Pointer to sqlite3_enable_XXX function */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  sqlite3_vfs_unregister(sqlite3_demovfs());
  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest_demovfs_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "register_demovfs", register_demovfs, 0, 0);
  Tcl_CreateObjCommand(interp, "unregister_demovfs", unregister_demovfs, 0, 0);
  return TCL_OK;
}

#else
int Sqlitetest_demovfs_Init(Tcl_Interp *interp){ return TCL_OK; }
#endif

#endif /* SQLITE_TEST */