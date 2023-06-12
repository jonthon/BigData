======
MOTIVE:
======
The main motive for this package is implementation of in-place data operations (ie. dropping duplicates) on very large files of data. For instance, ``pandas.read_*`` loading interfaces load large data files in chunks in runtime memory but don't provide a direct tool for in-place operations on those files.

It solves the problem by dumping data chunks marked with unique pathnames to disk memory, hence better data management on large files.


===========
DESCRIPTION:
===========
+ system interfaces: ``BigData``, ``Chunks``, ``ParallelRepeat``, and ``ParallelOnce``
+ pandas interfaces: ``PandasIO``

``BigData`` types perform data operations (not in-place) on the iterated data file in chunks in runtime memory. Whereas, ``Chunks``, ``ParallelRepeat``, and ``ParallelOnce`` perform all operations that ``BigData`` can and all in-place data operations on a data file in chunks on disk memory. Thus, data handling in this package is approached in two ways: data file as a whole (without in-place operations, ``pandas`` default) and data file in chunks paths (with in-place operations).

``Parallel*`` types are ideal for in-place operations on data chunks stored in a hierarchical directory. The chunks paths are sorted for integrity and passed to ``onparallel`` and ``onparallelonce`` methods of ``ParallelRepeat`` and ``ParallelOnce`` classes respectively in pairs. ``ParallelRepeat`` performs operations on data chunks pairs repeatedly while ``ParallelOnce`` performs operations once per unique pair. For instance, ``ParallelRepeat`` types, at some point a pair ``(path_x, path_y)`` will be operated and at a later point a pair ``(path_y, path_x)`` will be operated again; hence the repitition. In contrast, operations on same pairs in ``ParallelOnce`` are automatically skipped. Please, see the 'Dropping duplicates ...' in the examples section for a ``Parallel*`` use case.

``PandasIO`` class intercepts ``pandas.read_*`` and ``obj.to_*`` loading and dumping (IO) interfaces. It inherits all ``pandas`` IO features, however, it can optionally take ``chunksize`` argument in MB instead of lines and automatically convert it to lines. Thus, if ``mb`` argument of ``read_*`` is ``True``, then ``chunksize`` argument is expected to be in MB and returns (``pandas`` reader generator, number of chunks, and number of lines per chunk) tuple. 

For simplicity, a user need not worry about system management invoked in this package but performing data operations (ie. fetching, storing, formatting, cleaning, reshaping, aligning, aggregating, analysing, visualising, etc...) on a data file as a whole (without in-place) or in chunks (with in-place). 


========
EXAMPLES:
========

*sample data file*

.. code-block:: python

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


- output

dumb.pd



*Chunking a huge data file into chunks (non in-place)*

.. code-block:: python

	# Chop data into chunks
	class ChunkIt(mgr.BigData):
    	operation = 'Chunking ...'                 # for verbosity
    
    	# called in __init__ implicitly
    	def init(self):
        	pdIO = mgr.PandasIO(verbosity=True)
        	# if mb=True, else pandas defaults
        	data, nchunks, nlines = pdIO.read_json(file, mb=True, 
                	                               chunksize=0.005, 
                        	                       lines=True)
        	self.operate(data, chunksdir, nchunks)
        
    	def onchunkdata(self, data, chunkpath):
        	# more data operations here
        	data.to_json(chunkpath, lines=True, orient='records')
	# run
	ChunkIt(verbosity=2)

	# peek
	print('tree ...')
	!tree


- output

::

	counting ...
	=> file path  : dumb.pd
	   file size  : 22002 MB
	   chunks     : 5
	   nlines     : 29
	Chunking ...
		 chunk: [ 1 ]
		 chunk: [ 2 ]
		 chunk: [ 3 ]
		 chunk: [ 4 ]
		 chunk: [ 5 ]
	=> chunks     : 5
	   time taken : 0 days, 0 hrs, 0 mins, 0.07 secs
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



*Dropping duplicates on chunks of data saved in disk memory (in-place)*

.. code-block:: python

	# drop duplicates
	class DropDup(mgr.ParallelOnce):
    	operation = 'Dropping Duplicates ...'         # for verbosity
    
    	# called in __init__ implicitly
    	def init(self):
        	# in-place operation (file)
        	self.operate(chunksdir, file, True)
        
        	# prove operation accuracy
        	data2 = pd.read_json(file, lines=True)
        	if len(data2) == len(data):
            	print('drop duplicates PASSED!')
        	else:
            	print('drop duplicates FAILED!')
            
    	def onparallelonce(self, selfpath, parallelpath):
        	# operate on self data chunk
        	if selfpath == parallelpath:
            	data = self.loadself(selfpath)
            	data.drop_duplicates(inplace=True)
            	self.dumpself(data)
            	self.data = data
            	return
        	# operate on parallel data chunk
        	df2 = self.loadparallel(parallelpath)
        	if self.data.empty or df2.empty: return
        	df  = pd.concat([self.data, df2], keys=['df1', 'df2'])
        	dup = df.duplicated()
        	dup = dup.loc['df2']
        	df2 = df2[~dup]
        	self.dumpparallel(df2)
            
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


- output

Dropping Duplicates ...
	 chunkpath: [ dumb_dir/dumb_dir-1 ]
	 chunkpath: [ dumb_dir/dumb_dir-2 ]
	 chunkpath: [ dumb_dir/dumb_dir-3 ]
	 chunkpath: [ dumb_dir/dumb_dir-4 ]
	 chunkpath: [ dumb_dir/dumb_dir-5 ]
=> chunks     : 5
   time taken : 0 days, 0 hrs, 0 mins, 0.15 secs
joining   ...
cleaning  ...
done!


drop duplicates PASSED!
