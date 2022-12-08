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
** This file implements an example of a simple VFS implementation that 
** omits complex features often not required or not possible on embedded
** platforms.  Code is included to buffer writes to the journal file, 
** which can be a significant performance improvement on some embedded
** platforms.
**
** OVERVIEW
**
**   The code in this file implements a minimal SQLite VFS that can be 
**   used on Linux and other posix-like operating systems. The following 
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
**   It is assumed that the system uses UNIX-like path-names. Specifically,
**   that '/' characters are used to separate path components and that
**   a path-name is a relative path unless it begins with a '/'. And that
**   no UTF-8 encoded paths are greater than 512 bytes in length.
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
  sqlite3_io_methods const *pMethod;  /* Always the first entry */
  sqlite3_vfs *pVfs;                  /* The VFS that created this unixFile */

  const char* path;       /*path of the file*/
  int is_wal;             /*1 for wal file, 0 for database file*/
  int is_pmem;            /*1 if pmem, 0 otherwise*/
  size_t pmem_size;      /*The size of pmem-memory that was actually mapped, the pmem_file size*/
  char* pmem_file;        /*The entire pmem fiel represented as char array*/
  char *aBuffer;                  /* Pointer to malloc'd buffer */
  int nBuffer;                    /* Valid bytes of data in zBuffer */
  PMEMlogpool *log_pool;    /* The pool for log-pmem*/
  sqlite3_int64 iBufferOfst;      /* Offset in file of zBuffer[0] */
};

/*
** Write directly to the file passed as the first argument. Even if the
** file has a write-buffer (Persistent_File.aBuffer), ignore it.
*/
static int pmem_direct_write(
  Persistent_File *p,                    /* File handle */
  const void *zBuf,               /* Buffer containing data to write */
  int amt,                       /* Size of data to write in bytes */
  sqlite_int64 offset              /* File offset to write to */
){  
   printf("direct write\n");
  /*Check if one decides to write beyond the file end*/
  if(offset + amt > PMEM_LEN){
    return SQLITE_IOERR_WRITE;
  }


  if(p->is_pmem){
    pmem_memcpy(p->pmem_file + offset, zBuf, amt, PMEM_F_MEM_NONTEMPORAL);
  }
  else{
    memcpy(p->pmem_file, zBuf, amt);

  }



  strncpy(p->pmem_file+offset, (char*)zBuf, amt);

  /*make sure the wal is always persistent*/
  if (p->is_pmem && p->is_wal)
		pmem_persist(p->pmem_file,p->pmem_size);
	else
		pmem_msync(p->pmem_file,p->pmem_size);
  /*
  if(pmem_unmap(pmem_addr,mapped_len)){
    return SQLITE_IOERR_WRITE;
  }
  */
  return SQLITE_OK;
}

/*
** Flush the contents of the aBuffer buffer to disk. This is a
** no-op if this particular file does not have a buffer (i.e. it is not
** a journal file) or if the buffer is currently empty.
*/
static int pmem_flush_buffer(Persistent_File *p){
   printf("flush buffer\n");
  int rc = SQLITE_OK;
  printf("%i\n", p->nBuffer);
  if( p->nBuffer ){
    rc = pmem_direct_write(p, p->aBuffer, p->nBuffer, p->iBufferOfst);
    p->nBuffer = 0;
  }
  return rc;
}

/**
 * writes the buffer to pmem
 * no need to close anything since mappings are unmapped after writing
 * this function just frees the buffer
*/
static int pmem_close(sqlite3_file *pFile){
   printf("close\n");
  int rc;
  Persistent_File *p = (Persistent_File*)pFile;
  rc = pmem_flush_buffer(p);
  sqlite3_free(p->aBuffer);
  
  if(p->is_pmem){
    if(pmem_unmap(p->pmem_file,p->pmem_size)){
      return SQLITE_IOERR_WRITE;
  }
  }
  return rc;
}

/*
** Read data from a file.
*/
static int pmem_read(
  sqlite3_file *pFile,  /* the file*/
  void *zBuf, /* the buffer to write the value to */
  int iAmt, /* the size of the buffer */
  sqlite_int64 iOfst /*the offset to read */
){
  printf("read\n");
  Persistent_File *p = (Persistent_File*)pFile;
  off_t ofst;                     /* Return value from lseek() */
  int nRead;                      /* Return value from read() */
  int rc;                         /* Return code from demoFlushBuffer() */

  /* Flush any data in the write buffer to disk in case this operation
  ** is trying to read data the file-region currently cached in the buffer.
  ** It would be possible to detect this case and possibly save an 
  ** unnecessary write here, but in practice SQLite will rarely read from
  ** a journal file when there is data cached in the write-buffer.
  */
  rc = pmem_flush_buffer(p);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  if(iOfst + iAmt > PMEM_LEN){
    return SQLITE_IOERR_READ;
  }

  char *pmem_addr;
  size_t mapped_len;
  int is_pmem;

  /* open the pmem file to read back the data */
  if ((pmem_addr = (char *)pmem_map_file(p->path, PMEM_LEN, PMEM_FILE_CREATE,
        0666, &mapped_len, &is_pmem)) == NULL) {
    perror("pmem_map_file");
  }

  strncpy((char*)zBuf, pmem_addr+iOfst, iAmt);


  return SQLITE_OK;
}


/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
static int pmem_write (
  sqlite3_file *pFile,
  const void *zBuf,  
  int amt, 
  sqlite_int64 offset
){
  printf("write\n");
  Persistent_File *p = (Persistent_File*)pFile;
  
  assert ( pFile );
  assert( amt > 0);

  if(p->is_pmem && p->is_wal){
    if (pmemlog_append (p->log_pool, zBuf, amt) < 0) {
        printf("pmemlog_append: error occured\n");
        return -1;
    }
  }
  else{
    if(offset + amt <= p->pmem_size){
      if(p->is_pmem){
        /* automatically flushes data to pmem no extra call needed*/
        pmem_memcpy(p->pmem_file + offset, zBuf, amt, PMEM_F_MEM_NONTEMPORAL);
      }
      else{
        /* doesn't sync changes and therefor must be synced */
        memcpy(&p->pmem_file[offset], zBuf, amt);
        pmem_msync(p->pmem_file, p->pmem_size);
      }
    }
  }

  return SQLITE_OK;
  
//  if( p->aBuffer ){
//    char *z = (char *)zBuf;       /* Pointer to remaining data to write */
//    int n = amt;                 /* Number of bytes at z */
//    sqlite3_int64 i = offset;      /* File offset to write to */
//
//    while( n>0 ){
//      int nCopy;                  /* Number of bytes to copy into buffer */
//
//      /* If the buffer is full, or if this data is not being written directly
//      ** following the data already buffered, flush the buffer. Flushing
//      ** the buffer is a no-op if it is empty.  
//      */
//      if( p->nBuffer==SQLITE_DEMOVFS_BUFFERSZ || p->iBufferOfst+p->nBuffer!=i ){
//        int rc = pmem_flush_buffer(p);
//        if( rc!=SQLITE_OK ){
//          return rc;
//        }
//      }
//      assert( p->nBuffer==0 || p->iBufferOfst+p->nBuffer==i );
//      p->iBufferOfst = i - p->nBuffer;
//
//      /* Copy as much data as possible into the buffer. */
//      nCopy = SQLITE_DEMOVFS_BUFFERSZ - p->nBuffer;
//      if( nCopy>n ){
//        nCopy = n;
//      }
//      memcpy(&p->aBuffer[p->nBuffer], z, nCopy);
//      p->nBuffer += nCopy;
//
//      n -= nCopy;
//      i += nCopy;
//      z += nCopy;
//    }
//  }else{
//    return pmem_direct_write(p, zBuf, iAmt, iOfst);
//  }
  
}

/*
** Truncate a file. This is a no-op for this VFS (see header comments at
** the top of the file).
*/
static int pmem_truncate(sqlite3_file *pFile, sqlite_int64 size){
  printf("truncate\n");
#if 0
  if( ftruncate(((DemoFile *)pFile)->fd, size) ) return SQLITE_IOERR_TRUNCATE;
#endif
  return SQLITE_OK;
}

/*
** Sync the contents of the file to the persistent media.
*/
static int pmem_sync(sqlite3_file *pFile, int flags){
  printf("pmem sync\n");
  Persistent_File *p = (Persistent_File*)pFile;
  /* wal file is always written back after a write, no need to sync*/
  if(p->is_wal){
    return SQLITE_OK;
  }

  /* sync file contents, don't know if this is actually needed since write always syncs but better be safe than sorry*/
  if(p->is_pmem){
    pmem_persist(p->pmem_file, p->pmem_size);
  }
  else{
    pmem_msync(p->pmem_file, p->pmem_size);
  }
  return SQLITE_OK;


}

/*
** Write the size of the file in bytes to *pSize.
*/
static int pmem_file_size(sqlite3_file *pFile, sqlite_int64 *pSize){
  printf("file size\n");
  Persistent_File *p = (Persistent_File*)pFile;

  /* Flush the contents of the buffer to disk. As with the flush in the
  ** demoRead() method, it would be possible to avoid this and save a write
  ** here and there. But in practice this comes up so infrequently it is
  ** not worth the trouble.
  */
  int rc;
  rc = pmem_flush_buffer(p);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  *pSize = PMEM_LEN;
  return SQLITE_OK;
}

/*
** Locking functions. The xLock() and xUnlock() methods are both no-ops.
** The xCheckReservedLock() always indicates that no other process holds
** a reserved lock on the database file. This ensures that if a hot-journal
** file is found in the file-system it is rolled back.
*/
static int pmem_lock(sqlite3_file *pFile, int eLock){
  printf("lock\n");
  return SQLITE_OK;
}
static int pmem_unlock(sqlite3_file *pFile, int eLock){
  printf("unlock\n");
  return SQLITE_OK;
}
static int pmem_check_reserved_lock(sqlite3_file *pFile, int *pResOut){
  printf("reserved lock\n");
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** No xFileControl() verbs are implemented by this VFS.
*/
static int pmem_file_control(sqlite3_file *pFile, int op, void *pArg){
  printf("file control\n");
  return SQLITE_NOTFOUND;
}

/*
** The xSectorSize() and xDeviceCharacteristics() methods. These two
** may return special values allowing SQLite to optimize file-system 
** access to some extent. But it is also safe to simply return 0.
*/
static int pmem_sector_size(sqlite3_file *pFile){
  printf("sector size\n");
  return 0;
}
static int pmem_device_characteristics(sqlite3_file *pFile){
  printf("device characteristics\n");
  return 0;
}

/*
** Open a file handle.
*/
static int pmem_open(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zName,              /* File to open, or 0 for a temp file */
  sqlite3_file *pFile,            /* Pointer to DemoFile struct to populate */
  int flags,                      /* Input SQLITE_OPEN_XXX flags */
  int *pOutFlags                  /* Output SQLITE_OPEN_XXX flags (or NULL) */
){
  printf("open\n");
  static const sqlite3_io_methods pmem_io = {
    1,                            /* iVersion */
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
    pmem_device_characteristics     /* xDeviceCharacteristics */
  };

  Persistent_File *p = (Persistent_File*)pFile; /* Populate this structure */
  

  int oflags = 0;                 /* flags to pass to open() call */
  char *aBuf = 0;
  int eType = flags&0x0FFF00;  /* Type of file to open */
  int noLock;                    /* True to omit locking primitives */
  int rc = SQLITE_OK;            /* Function Return Code */
  int ctrlFlags = 0;             /* UNIXFILE_* flags */


  if( zName==0 ){
    return SQLITE_IOERR;
  }

  if( flags&SQLITE_OPEN_MAIN_JOURNAL ){
    aBuf = (char *)sqlite3_malloc(SQLITE_DEMOVFS_BUFFERSZ);
    if( !aBuf ){
      return SQLITE_NOMEM;
    }
  }

  int isExclusive  = (flags & SQLITE_OPEN_EXCLUSIVE);
  int isDelete     = (flags & SQLITE_OPEN_DELETEONCLOSE);
  int isCreate     = (flags & SQLITE_OPEN_CREATE);
  int isReadonly   = (flags & SQLITE_OPEN_READONLY);
  int isReadWrite  = (flags & SQLITE_OPEN_READWRITE);

  /* Check the following statements are true: 
  **
  **   (a) Exactly one of the READWRITE and READONLY flags must be set, and 
  **   (b) if CREATE is set, then READWRITE must also be set, and
  **   (c) if EXCLUSIVE is set, then CREATE must also be set.
  **   (d) if DELETEONCLOSE is set, then CREATE must also be set.
  */
  assert((isReadonly==0 || isReadWrite==0) && (isReadWrite || isReadonly));
  assert(isCreate==0 || isReadWrite);
  assert(isExclusive==0 || isCreate);
  assert(isDelete==0 || isCreate);

  /* The main DB, main journal, WAL file and super-journal are never 
  ** automatically deleted. Nor are they ever temporary files.  */
  assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MAIN_DB );
  assert( (!isDelete && zName) || eType!=SQLITE_OPEN_WAL );

  /* Assert that the upper layer has set one of the "file-type" flags. */
  assert( eType==SQLITE_OPEN_MAIN_DB      || eType==SQLITE_OPEN_TEMP_DB 
       || eType==SQLITE_OPEN_MAIN_JOURNAL || eType==SQLITE_OPEN_TEMP_JOURNAL 
       || eType==SQLITE_OPEN_SUBJOURNAL   || eType==SQLITE_OPEN_SUPER_JOURNAL 
       || eType==SQLITE_OPEN_TRANSIENT_DB || eType==SQLITE_OPEN_WAL
  );




  /*
  if( flags&SQLITE_OPEN_EXCLUSIVE ) oflags |= O_EXCL;
  if( flags&SQLITE_OPEN_CREATE )    oflags |= O_CREAT;
  if( flags&SQLITE_OPEN_READONLY )  oflags |= O_RDONLY;
  if( flags&SQLITE_OPEN_READWRITE ) oflags |= O_RDWR;
  */


  /* completly zeros p*/
  memset(p, 0, sizeof(Persistent_File));
  p->path = zName;
  p->pVfs = pVfs;
  p->pMethod = &pmem_io;
  p->aBuffer = aBuf;

  if ((p->pmem_file = (char *)pmem_map_file(p->path, PMEM_LEN, PMEM_FILE_CREATE,
        0666, &p->pmem_size, &p->is_pmem)) == NULL) {
    return SQLITE_NOMEM;
  }

  if( pOutFlags ){
    *pOutFlags = flags;
  }

  printf("open finished\n");
  return SQLITE_OK;
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
** Argument zPath points to a nul-terminated string containing a file path.
** If zPath is an absolute path, then it is copied as is into the output 
** buffer. Otherwise, if it is a relative path, then the equivalent full
** path is written to the output buffer.
**
** This function assumes that paths are UNIX style. Specifically, that:
**
**   1. Path components are separated by a '/'. and 
**   2. Full paths begin with a '/' character.
*/
static int demoFullPathname(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zPath,              /* Input path (possibly a relative path) */
  int nPathOut,                   /* Size of output buffer in bytes */
  char *zPathOut                  /* Pointer to output buffer */
){
  char zDir[MAXPATHNAME+1];
  if( zPath[0]=='/' ){
    zDir[0] = '\0';
  }else{
    if( getcwd(zDir, sizeof(zDir))==0 ) return SQLITE_IOERR;
  }
  zDir[MAXPATHNAME] = '\0';

  sqlite3_snprintf(nPathOut, zPathOut, "%s/%s", zDir, zPath);
  zPathOut[nPathOut-1] = '\0';

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
static void *demoDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return 0;
}
static void demoDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
  zErrMsg[nByte-1] = '\0';
}
static void (*demoDlSym(sqlite3_vfs *pVfs, void *pH, const char *z))(void){
  return 0;
}
static void demoDlClose(sqlite3_vfs *pVfs, void *pHandle){
  return;
}


/*
** Parameter zByte points to a buffer nByte bytes in size. Populate this
** buffer with pseudo-random data.
*/
static int demoRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte){
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
sqlite3_vfs *sqlite3_pmem_vfs(void){
  static sqlite3_vfs pmem_vfs = {
    1,                            /* iVersion */
    sizeof(Persistent_File),             /* szOsFile */
    MAXPATHNAME,                  /* mxPathname */
    0,                            /* pNext */
    "Pmem_VFS",                       /* zName */
    0,                            /* pAppData */
    pmem_open,                     /* xOpen */
    demoDelete,                   /* xDelete */
    unixAccess,                   /* xAccess */
    demoFullPathname,             /* xFullPathname */
    demoDlOpen,                   /* xDlOpen */
    demoDlError,                  /* xDlError */
    demoDlSym,                    /* xDlSym */
    demoDlClose,                  /* xDlClose */
    demoRandomness,               /* xRandomness */
    unixSleep,                    /* xSleep */
    demoCurrentTime,              /* xCurrentTime */
    unixGetLastError,     /* xGetLastError */               \
    unixCurrentTimeInt64, /* xCurrentTimeInt64 */           \
    unixSetSystemCall,    /* xSetSystemCall */              \
    unixGetSystemCall,    /* xGetSystemCall */              \
    unixNextSystemCall,   /* xNextSystemCall */             \
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