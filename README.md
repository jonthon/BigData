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


EXAMPLES:
--------
- Chunking a huge data file into chunks (non in-place)

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
data1 = pd.concat([data, data.iloc[:25]])        # duplicate
shuff = np.random.permutation(len(data1))        # shuffler
data1 = data1.take(shuff)			 # shuffle
data1.to_json(file, lines=True, orient='records')

# peek
!ls

# Chop data into chunks
class ChunkIt(mgr.BigDataPd):
    operation = 'Chunking ...'  # for verbosity
    
    def init(self):
        # if mb=True, else pandas defaults
        data, nchunks, nlines = self.read_json(file, mb=True, 
						     chunksize=0.005, 
						     lines=True)
        self.operate(data, chunksdir, nchunks)
	
    def onchunkdata(self, data, chunkpath):
        # more data operations here
        self.to_json(data, chunkpath, lines=True, orient='records')
# run
ChunkIt(verbosity=2)


# peek
print('tree ...')
!tree

```

- output

```
dumb.pd

counting ...
=> file path  : dumb.pd
   file size  : 21970 MB
   chunks     : 5
   nlines     : 29
Chunking ...
	 chunk: [ 1 ]
	 chunk: [ 2 ]
	 chunk: [ 3 ]
	 chunk: [ 4 ]
	 chunk: [ 5 ]
=> chunks     : 5
   time taken : 0 days, 0 hrs, 0 mins, 0.05 secs
done!

tree ...
.
├── dumb_dir
│   ├── dumb_dir-1
│   ├── dumb_dir-2
│   ├── dumb_dir-3
│   ├── dumb_dir-4
│   └── dumb_dir-5
└── dumb.pd
1 directory, 6 files

```


- Dropping duplicates on chunks of data saved in disk memory (in-place).

```
# drop duplicates
class DropDup(mgr.DropDuplicatesPd):
    operation = 'Dropping Duplicates ...'	# for verbosity
    
    def init(self):
        # in-place operation (file)
        self.operate(chunksdir, file, True)
	
        # prove operation accuracy
        data2 = pd.read_json(file, lines=True)
	if len(data2) == len(data):
            print('drop duplicates PASSED!')
        else:
            print('drop duplicates FAILED!')
	    
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

- output

```
Dropping Duplicates ...
	 chunkpath: [ dumb_dir/dumb_dir-1 ]
	 chunkpath: [ dumb_dir/dumb_dir-2 ]
	 chunkpath: [ dumb_dir/dumb_dir-3 ]
	 chunkpath: [ dumb_dir/dumb_dir-4 ]
	 chunkpath: [ dumb_dir/dumb_dir-5 ]
=> chunks     : 5
   time taken : 0 days, 0 hrs, 0 mins, 0.11 secs
joining   ...
cleaning  ...
done!

drop duplicates PASSED!
```
