# DataMgr

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
- example code

```
import numpy   as np
import pandas  as pd
import datamgr as mgr

file      = 'dumb.pd'
chunksdir = 'dumb_dir'

# create data
data  = np.random.randn(1000).reshape((100, 10))
data  = pd.DataFrame(data)
data.drop_duplicates(inplace=True)
data1 = pd.concat([data, data.iloc[:25]])       # duplicate
data1.sample(frac=1).reset_index(drop=True)      # shuffle
data1.to_json(file, lines=True, orient='records')

# peek
!ls

# Chop data into chunks
print()
print()
print()
class ChunkIt(mgr.BigDataPd):
    operation = 'Chunking ...'  # for verbosity
    def init(self):
        # if mb=True, else pandas defaults
        data, nchunks, nlines = self.read_json(file, mb=True, chunksize=0.005, lines=True)
        self.operate(data, chunksdir, nchunks)
    def onchunkdata(self, data, chunkpath):
        # more data operations here
        self.to_json(data, chunkpath, lines=True, orient='records')
# run
ChunkIt(verbosity=2)

# peek
print('tree ...')
!tree

# drop dupplicates
print()
print()
print()
class DropDup(mgr.DropDuplicatesPd):
    operation = 'Dropping Duplicates ...'
    def init(self):
        # in-place operation (file)
        self.operate(chunksdir, file, True)
        # prove operation accuracy
        try:
            data2 = pd.read_json(file, lines=True)
            pd.testing.assert_frame_equal(data, data2)
        except AssertionError:
            print('drop duplicates FAILED!')
        else:
            print('drop duplicates PASSED!')
    def loadself(self, selfpath):
        self.selfpath = selfpath
        return pd.read_json(selfpath, lines=True)
    def dumpself(self, selfdata):
        selfdata.to_json(self.selfpath, lines=True, orient='records')
    def loadparallel(self, parallelpath):
        self.parallelpath = parallelpath
        return pd.read_json(parallelpath, lines=True)
    def dumpparallel(self, paralleldata):
        paralleldata.to_json(self.parallelpath, lines=True, orient='records')

# run
DropDup(verbosity=2)
```
