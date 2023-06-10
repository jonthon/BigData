# BigData

MOTIVE:
------
The main motive for this package is implementation of in-place data operations (ie. dropping duplicates) on very large files of data. For instance, ```pandas.read_*``` loading interfaces load large data files in chunks in runtime memory but don't provide a direct tool for in-place operations on those files.

It solves the problem by dumping data chunks marked with unique pathnames to disk memory, hence better data management on large files.


DESCRIPTION:
-----------
+ system interfaces: ```BigData```, ```Chunks```, ```ParallelRepeat```, and ```ParallelOnce```
+ pandas interfaces: ```Pandas```, ```BigDataPd```, ```ChunksPd```, ```ParallelPdRepeat```, ```ParallelPdOnce```, ```SamplePd```, and ```DropDuplicatesPd```

```BigData*``` types perform data operations (not in-place) on the iterated data file in chunks in runtime memory. Whereas, ```Chunks*```, ```ParallelRepeat*```, and ```ParallelOnce*``` perform all operations that ```BigData*``` can and all in-place data operations on a data file in chunks on disk memory. Thus, data handling in this package is approached in two ways: data file as whole (without in-place operations, ```pandas``` default) and data file in chunks paths (with in-place operations).

Support is provided for data files in formats (ie. json, html, csv, table, etc) supported by data libraries like ```pandas```, ```numpy```, ```polars```, etc. However, only ```pandas``` loading and dumping (```read_*``` and ```obj.to_*```) interfaces are implemented in this package. For implementation of other data library interfaces, customization is more than welcomed. The low level types (system intefaces) mentioned above are the recommended interfaces to customize for implementation of other specific data library. See pandas interfaces mentioned above in the ```__init__.py``` for an example implementation.

For simplicity, a user need not worry about system management invoked in this package but perform operations on files as a whole (without in-place) or in chunks (with in-place). These data operations include: storing, formatting, fetching, cleaning, reshaping, aligning, aggregating, analysing, visualising, etc. 


USAGE:
-----
The ```if __name__ == '__main__':``` statement block in ```__init__.py``` implements unittests for this package's main logic (handling big files or hierarchical chunks of data). In these tests, pandas specific interfaces are customized and invoked as desired. Please, see code file (```__init__.py```) for example usages.


EXAMPLES:
--------
- Sample output from unittest run: 
   Three intial test files ```test1.json```, ```test2.json```, and ```test3.json``` are chunked into a hierarchical directory. Then, duplicates are dropped from all chunks (45). Please, see below sample output with lowest verbosity from the ```test_parallelization``` test suite. 

```
jon@jons-linux:~$ python3 -W ignore lib/datamgr/__init__.py
Chunking .....
..counting ...
=> file path  : test1.json
   file size  : 7329105 MB
   chunks     : 15
   nlines     : 2843
operating ...
=> chunks     : 15
   time taken : 0 days, 0 hrs, 0 mins, 0.31 secs
done!


counting ...
=> file path  : test2.json
   file size  : 7328699 MB
   chunks     : 15
   nlines     : 2843
operating ...
=> chunks     : 15
   time taken : 0 days, 0 hrs, 0 mins, 0.34 secs
done!


counting ...
=> file path  : test3.json
   file size  : 7329145 MB
   chunks     : 15
   nlines     : 2843
operating ...
=> chunks     : 15
   time taken : 0 days, 0 hrs, 0 mins, 0.33 secs
done!

Dropping Dupplicates ...
operating ...
=> chunks     : 45
   time taken : 0 days, 0 hrs, 0 mins, 21.3 secs
joining   ...
cleaning  ...
done!


.
----------------------------------------------------------------------
Ran 3 tests in 25.487s

OK
```
