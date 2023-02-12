This repository contains the code for my bachelors thesis.

# How to build
I made a cmake file so do the magic:
```
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```
The cmake supports Debug and Release mode, so set the -CMAKE_BUILD_TYPE accordingly.

## How to run all tests
First build the everything.
All scripts are copied into the build folder.
Modify the permissions of the script `all.sh`:
```
chmod u+x all.sh
./all.sh
```
Then use `./all.sh` to run the test.
TATP, SSB and Blob can also be launched one by one
with the script in their respective folder.
All test results are stored into the `master_results.csv`
in the results folder.

# different programs which are implemented
## TATP
-   __tatp_duckdb__
-   __tatp_msc_dense__
-   __tatp_msc_large__
-   __tatp_sqlite__
## SSB
-   __ssb_duckdb__
-   __ssb_msc_dense__
-   __ssb_msc_large__
-   __ssb_sqlite3__
## Blob
-   __blob_duckdb__
-   __blob_msc_dense__
-   __blob_msc_large__
-   __blob_sqlite__
## SQLite
-   __sqlite3_shell__

# Sources
- sqlite3 from sqlite.org
- DuckDB from https://github.com/UWHustle/sqlite-past-present-future
- benchmark setup from: https://github.com/UWHustle/sqlite-past-present-future

# How to find changes
All relevant changes in for MSC-log-dense and MSC-log-large
are tagged with __MSC__ in their respective `sqlite3.c` file

Author
=============
[Manuel Scheinost](mailto:manuel.scheinost@tum.de)