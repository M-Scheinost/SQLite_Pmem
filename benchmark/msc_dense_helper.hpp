#include "../sqlite/msc-log-dense/sqlite3.h"
#include "../vfs/pmem_vfs.h"

namespace std{


sqlite3* open_db(const char* path, string pmem){
  sqlite3 *db;
  int rc = sqlite3_initialize();
  if(rc){cout << "Init not working: " << rc << endl;}
  int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE;
  if(pmem == "PMem" || pmem == "pmem-nvme"){
    sqlite3_vfs_register(sqlite3_pmem_vfs(), 0);
    rc = sqlite3_open_v2(path, &db, flags, "PMem_VFS");
  }
  else{
    rc = sqlite3_open_v2(path, &db, flags, "unix");
  }
  if(rc){cout <<"Open:\t" << rc << endl;}
  rc = sqlite3_exec(db,"PRAGMA journal_mode=WAL", NULL,NULL,NULL);
  if(rc){cout << "Pragma WAL not working: " << rc << endl;}
  rc = sqlite3_exec(db,"PRAGMA synchronous=FULL", NULL,NULL,NULL);
  if(rc){cout << "Pragma synchronous not working: " << rc << endl;}

  // rc = sqlite3_exec(db,"PRAGMA mmap_size=0", NULL,NULL,NULL);
  // if(rc){cout << "Pragma mmap_size not working: " << rc << endl;}
  rc = sqlite3_exec(db,"PRAGMA cache_size=0", NULL,NULL,NULL);
  if(rc){cout << "Pragma cache_size not working: " << rc << endl;}
  
  return db;
}


void close_db(sqlite3* db){
  int rc;
  int *frames;
  
  //rc = sqlite3_exec(db,"wal_checkpoint(FULL)", NULL,NULL,NULL);
  //if(rc){cout << "Pragma wal_checkpoint not working: " << rc << endl;}

  rc = sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_TRUNCATE, NULL, frames);
  if(rc){cout <<"WAL-Checkpoint: " << rc << endl;}

  rc = sqlite3_close_v2(db);
  if(rc){cout <<"Close:\t" << rc << endl;}
  rc = sqlite3_shutdown();
  if(rc){cout << "Shutdown not working: " << rc << endl;}
}

} // namespace std