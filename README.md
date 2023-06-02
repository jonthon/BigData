# bigdata

DESCRIPTION
-----------
This package is responsible for handling data analysis and management of huge data files (any size). There's no limit to how much data size this package can handle (see package __init__.py file for more). 

It chops a file of any size into chunks and lets a user perform any data operation (dropping duplicates, getting samples, formatting, data integrity, etc..) on the whole data file in chunks. Please see BigDataPd, ChunksPd, ParallelPdRepeat, ParallelPdOnce, SamplePd, and DropDuplicatesPd classes for pandas specific interfaces. The lower classes that derive these pandas specific interfaces (with similar naming conventions) can be used to derive numpy, polars, or any other data management interface.

This package implicitly integrates system interfaces that handle filesystem management and leaves its users to only worry on specific data operations. 

Please refer to the code for more.
