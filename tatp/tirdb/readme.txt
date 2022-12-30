TATP Benchmark
Test Input and Result Database (TIRDB)
2009-03-26

General
----------
This directory contains SQL scripts to set up and manipulate the TIRDB database.

The file list follows:

tirdb.sql            	     - the schema file of TIRDB
initDataExample.sql	     - example file to feed initial data to TIRDB. 
			       Change the values according to your environment
get_test_runs.sql            - get all test runs (with conditions)
get_all_90percentile.sql     - get all 90% reponse times for a give run
get_resp_time_histograms.sql - get the response time histograms


Installing TIRDB
----------------
Create the database using the DDL script (tirdb.sql) in any SQL database. 
Populate the following database tables:

OPERATING_SYSTEMS - the operating system of the benchmark hardware (Server)
_DATABASES - the information of the databases to be benchmarked
HARDWARE - general benchmark hardware specification (Server)

and optionally:

DISK_TYPES - hardware disk specs.
BUS_TYPES - hardware bus specs.
PROCESSOR_TYPES - hardware processor specs.
MEMORY_TYPES - hardware memory specs.

You can use the script initDataExample.sql from this folder and change 
the values according to your environment. The rest of the tables of the 
schema are manipulated by TATP benchmark Suite.

Result data
----------
The following database tables store the initialization and monitoring data 
for each benchmark session/run:
TEST_SESSIONS
TEST_RUNS
TRANSACTION_MIXES

The actual result data of the benchmark is stored to the following tables:
RESULT_RESPONSE_SCALE
RESULT_THROUGHPUT

For more detailed description of the result data see TATP - Test Suite Guide 
that can be found in /doc/TATP_Test_Suite_Guide.doc.
