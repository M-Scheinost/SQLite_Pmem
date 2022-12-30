README.txt
Telecom Application Transaction Processing (TATP) Benchmark v. 1.1.0 
Package build no. 1
Date 2011-07-06

Introduction 
------------ 

Telecom Application Transaction Processing (TATP) Benchmark Suite is a
database benchmark designed to generate a database request load
typical for a HLR (Home Location Register) database in the mobile
networks.

This software package includes all the necessary files to set up the
benchmark environment and to execute the benchmark. In addition to the
binary executables, the source code is present too.
Project files for Visual Studio 2008 are as well included.

Note on the executables in this package: the TATP binaries have been build with the Windows ODBC driver manager. TATP execution requires aprropriate data source names (DSNs) to be defined in the driver manager (with the ODBC control panel).

Directories
-----------
bin\	- executables for the benchmark suite + the following input files:
	  	- TATP initialization file (tatp.ini)
		- transaction files for different DBMSs (e.g. tr_mix_solid.sql)
		- target database initialization file (targetDBInit.sql)
		- a generic target database schema file (targetDBSchema.sql)
                - target database schema files for DB2 (targetDBSchema_db2.sql) 
                  and Informix (targetDBSchema_ids.sql)
                - an example Remote Nodes file (remoteNodes.ini)
                - an example solidDB client-side configuration file (solid.ini)
                - PCRE 6.4 DLL files (pcre.dll and pcreposix.dll)

ddf\	- example Data Definition Files

doc\	- user's guides (in PDF format)

images\ - HTML help files

source\ - source code, project files + PCRE files needed to
                  build the TATP programs
tdf\	- example Test Definition Files
tirdb\	- input/result database (TIRDB) initialization files

Root-level files
-----------------
README.txt               (this file)
index.html               Welcome page, start from here!
license_agreement.html   license texts
tatp_relnotes.html       Release Notes
tatp_faq.html            FAQ
quick.html               Quick Start Guide for TATP trial


Running a trial of TATP
-----------------------
To try the TATP Suite software in a single computer, read 'quick.html'

Preparing to run production TATP tests
--------------------------------------
The 'bin\' directory contains the executables and a set of input
files needed to run the TATP benchmark. Of the input files, tatp.ini
and a transaction file are mandatory to run the benchmark.

You need to define a DDF (Data Definition File, example files are
located in 'ddf\') and a TDF (Test Definition File, example
files are located in 'tdf\') to run a benchmark.

Prior to the first TATP run you may want to install the input/result
database (TIRDB). Any database product supporting an ODBC connection
can be used as TIRDB. Use the 'tirdb\tirdb.sql' script to
create the TIRDB. The script 'tirdb\initDataExample.sql' can
be used to feed initial data to TIRDB.

Running the benchmark
---------------------
The TATP benchmark can be run directly from the bin\
folder by issuing the command

  tatp <parameters> file.ddf file1.tdf file2.tdf ...

For more info, see 'index.html'

Building TATP from source code
------------------------------
Project files for Visual Studio 2008 are included. The solution file is called 
'tatp-VS2008.sln'. You can build both the 64b and 32b binaries by selecting
the platform designator "x64" or "WIN32", repectively. The available configurations
are:
- Release: a build with the Windows ODBC driver manager. At least one 
  ODBC driver for the target database should be installed, and data source
  names for the target database and TIRDB defined.
- Debug: same as above, a debug build
All the following configurations are solidDB-specific and require that
solidDB (32b or 64b) is installed on the system. Additionally, each of those 
configurations require a specific solidDB import library that has to be 
copied from the solidDB installation lib directory to the directories in'source':
- 32b libraries to  'lib_win32' 
- 64b libraries to 'lib_x64' . 
Alternatively, the library paths can be updated in the Visual Studio project files.
- ReleaseSolidODBC: a build using solidDB ODBC driver. Import library 
  'solidimpodbca.lib' is required.
- ReleaseSolidSMA: A build using solidDB SMA (shared memory access) driver. 
  Import library 'solidimpsma.lib' is required.
- ReleaseAccelerator: A build using solidDB LLA (linked library access) driver. 
  Import library solidimpac.lib is required.

Contact and support
-------------------

http://sourceforge.net/projects/tatpbenchmark/

Copyright IBM Corporation 2004, 2011.