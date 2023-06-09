# BigData

DESCRIPTION:
-----------
+ system interfaces: ```BigData```, ```Chunks```, ```ParallelRepeat```, and ```ParallelOnce```
+ pandas interfaces: ```Pandas```, ```BigDataPd```, ```ChunksPd```, ```ParallelPdRepeat```, ```ParallelPdOnce```, ```SamplePd```, and ```DropDuplicatesPd```

This package implements interfaces for use on huge data files of any format (ie. json, html, csv, table, etc) supported by data libraries like ```pandas```, ```numpy```, ```polars```, etc. ```pandas``` loading and dumping (```read_*``` and ```obj.to_*```) interfaces are primarily used in this package. However, customization is more than welcomed to implement other data library interfaces. The low level interfaces (system intefaces) mentioned above are the recommended interfaces to customize for a specific data library. See pandas interfaces mentioned above in the ```__init__.py``` for an example implementation.

It uses system interfaces to simplify data management; thus, a user only needs to perform specific data operations on files of any size that a running machine can hold. These specific data operations include fetching data, cleaning, reshaping, aligning, aggregating, analysing, visualising, etc. 

Data handling in this package is approached in two main ways, either from a huge file or from chunks of data. 

USAGE:
-----


EXAMPLES:
--------
