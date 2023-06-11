
import pandas as pd
import os, time

# don't relative import when unittesting or error
if __name__ != '__main__': from . import (_jsonmgr as json, 
                                          _pdmgr as pdmgr,)
# modules from this package used in unittesting
else: from datamgr import _pdmgr as pdmgr


#################################################################
# general 
#################################################################
class Timer:
    "run self.start, self.stop, then self.timetaken for elapsed time"
    def __init__(self):
        self.started = None
        self.stopped = None
    def start(self):
        self.started = time.perf_counter()
    def stop(self):
        self.stopped = time.perf_counter()
    def timetaken(self):
        DAYS = 24 * 3600, 'days'
        HRS  = 3600,      'hrs'  
        MINS = 60,        'mins'  
        SECS = 1,         'secs'
        timetaken = []
        secs = self.stopped - self.started
        for qty, unit in DAYS, HRS, MINS:
            qty, secs = divmod(secs, qty)
            timetaken.append('%s %s' % (int(qty), unit))
        timetaken.append('%s %s' % (round(secs, 2), SECS[1]))
        return ', '.join(timetaken)


#################################################################
# System interfaces
#################################################################
class FileSystemMgr:
    """
    This class is the base system interface with mixin interface 
    logic (ie. self.StopOperation, self.init) used in higher 
    system classes implemented in this package. It's recommended 
    not to customize directly for data specific operations.
    """
    operation = 'operating ...'

    class StopOperation(Exception): pass

    def __init__(self, *, verbosity=False):
        self.verbosity = verbosity
        self.init() # simplicity
    
    def init(self): pass

    def joinchunks(self, chunkspaths, opath):
        if not chunkspaths: return # save sys resources
        if self.verbosity: print('joining   ...')
        chunkspaths.sort()
        file = open(opath, 'wb')
        for chunkpath in chunkspaths:
            file.write(open(chunkpath, 'rb').read())
        file.close()

    def clean(self, chunksdir):
        if self.verbosity: print('cleaning  ...')
        os.system('rm -r %s' % chunksdir)

class BigData(FileSystemMgr):
    """
    This class emulates arrays scalar operations accross chunks of 
    huge data files instead of dimensions. Thus, same operations 
    are perfomed on every chunk of parsed data from a huge data file.

    Customize onchunkdata method for data operations (ie. counts, 
    reformatting, transforming, etc ...). onchunkdata takes a 
    data chunk and a path that can be used to dump the chunk to disk 
    memory for later operations.

    Calling the operate method, starts the scalar operations accross 
    chunks. For simplicity fetch desired data and call operation 
    method in init method (implicitly called by the constructor). 
        - operate args: 
            + data:      parsed data iterable (ie. pd chunks reader)
            + chunksdir: directory to dump chunks of data
            + nchunks:   precision of chunkspaths (ie.chunksdir-001) 
            + opath:     output file path to join chunks
            + clean:     removes chunksdir recursively if True
    """
    def operate(self, data, chunksdir, nchunks=None, opath=None, clean=False):
        """
        """
        if self.verbosity: print(self.operation)

        #**********************************************
        # operation handler logic, OOP as desired
        #**********************************************
        # make chunksdir if not yet 
        os.mkdir(chunksdir)           # check for existence?
        chunkname        = chunksdir.split(os.sep)[-1]
        chunkspaths      = []
        self.chunksdir   = chunksdir    # for higher classes use
        self.chunkspaths = chunkspaths  # for higher classes use
        timer = Timer() # create timer
        timer.start()   # start  timer
        try:
        # starts operation loop ..........................................
            for chunknum, chunkdata in enumerate(data, 1):
                if self.verbosity > 1: print('\t', 'chunk:', '[',chunknum,']')
                if nchunks:
                    chunkpath = str(len(str(nchunks))) # digits precision
                    chunkpath = chunkname + '-%0' + chunkpath + 'd'
                    chunkpath = chunkpath % chunknum
                else: 
                    chunkpath = chunkname + '-' + str(chunknum)
                chunkpath = os.path.join(chunksdir, chunkpath)
                chunkspaths.append(chunkpath)
                # operate on every chunk
                # stops operation loop or ................................
                self.onchunkdata(chunkdata, chunkpath)
        # completes operation loop .......................................
        except self.StopOperation: pass
        timer.stop()    # stop timer
        if self.verbosity:
            print('=> chunks     : %s' % chunknum)
            print('   time taken : %s' % timer.timetaken())

        if opath: self.joinchunks(chunkspaths, opath)

        if clean: self.clean(chunksdir)

        if self.verbosity: print('done!\n\n')
    def onchunkdata(self, data, chunkpath): raise NotImplementedError
    
class Chunks(FileSystemMgr):
    """
    Similar to BigData, this class emulates arrays scalar operations 
    accross chunks of data in a hierarchical directory of chunks 
    instead of a single file. Thus, same operations are perfomed on 
    every path of data chunk. 

    Customize onchunkpath method for data operations (ie. counts, 
    reformatting, transforming, etc ...). onchunkdata takes a path 
    to a data chunk that can be used to load the data in memory and 
    perform in-place operations on the chunk (just use same path to 
    dump back).

    Calling the operate method, starts the scalar operations accross 
    chunks. For simplicity, call operate with desired arguments in 
    init method (implicitly called by the constructor). 
        - operate args: 
            + chunksdir: hierarchical directory of data chunks 
            + opath:     output file path to join chunks
            + clean:     removes chunksdir recursively if True
    """
    def operate(self, chunksdir, opath=None, clean=False):
        if self.verbosity: print(self.operation)

        # collect paths for processing
        chunkspaths = []
        for dirname, subdirs, filenames in os.walk(chunksdir):
            if filenames: chunkspaths.extend([os.path.join(dirname, filename) 
                                            for filename in filenames])
        chunkspaths.sort()             # corrupts data if not called

        self.chunksdir   = chunksdir   # for use in higher classes (state)
        self.chunkspaths = chunkspaths # for use in higher classes (state)

        timer = Timer() # create timer
        timer.start()   # start  timer
        try:
        # starts operation loop .............................................
            for chunknum, chunkpath in enumerate(self.chunkspaths, 1):
                if self.verbosity > 1: print('\t', 'chunkpath:', '[',chunkpath,']')
                # stops operation loop or ...................................
                self.onchunkpath(chunkpath)
        # completes operation loop ..........................................
        except self.StopOperation: pass 
        timer.stop()    # stop timer
        if self.verbosity:
            print('=> chunks     : %s' % chunknum)
            print('   time taken : %s' % timer.timetaken())

        if opath: self.joinchunks(chunkspaths, opath)

        if clean: self.clean(chunksdir)

        if self.verbosity: print('done!\n\n')
    def onchunkpath(self, chunkpath): raise NotImplementedError

class ParallelRepeat(Chunks):
    """
    This class customizes onchunkpath method of Chunks class. It 
    re-iterates a hierarchical directory of data chunk paths for 
    every chunk path in the directoy repeatedly. 

    It pairs chunks paths of a directory and passes the path names 
    to onparallel method for pair operations. Thus, it performs 
    inter-paths operations of a hierarchical directory of data chunks, 
    however, repeatedly.

    Thus, if at one point (selfpath1, parallelpath1) pair is 
    performed, then at a later point (parallelpath1, selfpath1)
    pair will be performed; hence, the repitition.

    Customize on onparallel to implement data operations (ie. dropping 
    duplicates, etc ...). For simplicity, call operate in init method 
    (implicitly called by the constructor).
    - onparallel method args:
        + selfpath:     path that re-iterates the chunks hierarchy
        + parallelpath: a single path of the re-iteration by selfpath 


    * NOTE: performs (nchunks ** 2) loop runs
    """
    def onchunkpath(self, selfpath):
        # starts parallel operation loop ..........................
        for parallelpath in self.chunkspaths:
            # stops parallel operation loop or ....................
            self.onparallel(selfpath, parallelpath)
        # completes parallel operation ............................
    def onparallel(self, selfpath, parallelpath):
        raise NotImplementedError

class ParallelOnce(ParallelRepeat):
    """
    This class customizes onparallel method of ParallelRepeat class. It 
    re-iterates a hierarchical directory of data chunk paths for 
    every chunk path in the directoy. Unlike ParallelRepeat, it only 
    operates on a unique pair once. 

    It pairs chunks paths of a directory and passes the path names 
    to onparallelonce method for pair operations. Thus, it performs 
    inter-paths operations of a hierarchical directory of data chunks 
    once per each unique pair.

    Thus, if at one point (selfpath1, parallelpath1) pair is 
    performed, then at a later point (parallelpath1, selfpath1)
    pair will be skipped.

    Customize on onparallelonce to implement data operations 
    (ie. dropping duplicates, etc ...). For simplicity, call operate 
    in init method (implicitly called by the constructor).
    - onparallelonce method args:
        + selfpath:     path that re-iterates the chunks hierarchy
        + parallelpath: a single path of the re-iteration by selfpath 

    * NOTE: performs math.factorial(nchunks) loop runs
    """
    def onparallel(self, selfpath, parallelpath):
        if not parallelpath >= selfpath: return
        if self.verbosity > 2: 
            print('\t\t', 'parallelpath:', '[',parallelpath,']')
        self.onparallelonce(selfpath, parallelpath)
    def onparallelonce(self, selfpath, parallelpath): 
        raise NotImplementedError


################################################################
# pandas system interfaces, they customize BigData
################################################################
# pandas mixin
class PandasIO:
    "pandas loading and dumping (IO) interface mixin."
    def __init__(self, *, verbosity=False):
        self.verbosity = verbosity

    def __getattr__(self, attr):
        #****************************************************
        # intercepts pandas.read_* functions for file reading
        #****************************************************
        def read_(ipath, *, mb=False, **kwargs):
            "Intercepts pandas read_* to optionally take chunksize in MB"
            def mb_to_lines(ipath, chunksize):
                "Converts chunksize from MB to nlines"
                if self.verbosity: print('counting ...')
                import math
                import subprocess
                # file lines and size counts
                temp = subprocess.run(['wc', '-lc', ipath], 
                                    capture_output=True)
                flines, fsize = temp.stdout.decode().split()[:2]; del temp
                flines, fsize = int(flines), int(fsize)
                nlines = math.ceil(chunksize * (10 ** 6) * flines / fsize)
                # return (chunklines, nchunks) tuple 
                nchunks = math.ceil(flines / nlines)
                if self.verbosity: 
                    print('=> file path  : %s'    % ipath)
                    print('   file size  : %s MB' % fsize)
                    print('   chunks     : %s'    % nchunks)
                    print('   nlines     : %s'    % nlines)
                return nlines, nchunks
            
            # emulate pd chunksize protocol and keep the rest
            try:
                if mb and kwargs['chunksize']: 
                    chunksize           = kwargs['chunksize']
                    nlines, nchunks     = mb_to_lines(ipath, chunksize)
                    kwargs['chunksize'] = nlines
                else: raise KeyError
            # keep pandas' defaults
            except KeyError: chunksize  = None
            data = getattr(pd, attr)(ipath, **kwargs)
            if chunksize:
                if mb: return data, nchunks, nlines
                else:  return data
            return [data]

        #**************************************************
        # intercepts pandas.to_* write methods
        #**************************************************
        def to_(data, opath, **kwargs):
            "dumps pandas data structure to file in desired format"
            return getattr(data, attr)(opath, **kwargs)
        
        # calls respective pandas io interface
        if   'read_' in attr: return read_
        elif 'to_'   in attr: return to_
        raise AttributeError(self.__class__.__name__ + 
                             " doesn't have %s attribute" % attr) 


#################################################################
# pandas specific interfaces
#################################################################
class SamplePd(BigData):
    "Returns a sample of data with respect to passed df.dropna args"
    operation = 'sampling ...'
    def __init__(self, min, max, *, verbose=False, **kwargs):
        self.min, self.max = min, max
        self.kwargs = kwargs
        self.sample = None
        BigData.__init__(self, verbosity=verbose)
    def onchunkdata(self, data, chunkpath):
        chunk = data.dropna(**self.kwargs)
        if chunk.shape[0] >= self.min: 
            self.onsample(chunk.iloc[:self.max], chunkpath)
            raise self.StopOperation
    def onsample(self, sample, chunkpath): self.sample = sample

class Drop_DuplicatesPd(ParallelOnce):
    "pandas drop_dupilcate emulation for data chunks directory"
    operation = 'dropping duplicates ...'
    def onparallelonce(self, selfpath, parallelpath):
        if selfpath == parallelpath:
            data = self.loadself(selfpath)
            data.drop_duplicates(inplace=True)
            self.dumpself(data)
            self.data = data
            return
        df2      = self.loadparallel(parallelpath)
        if self.data.empty or df2.empty: return
        ign, df2 = pdmgr.drop_duplicates(self.data, df2)
        self.dumpparallel(df2)
    def loadself(    self, selfpath    ): raise NotImplementedError
    def dumpself(    self, selfdata    ): raise NotImplementedError    
    def loadparallel(self, parallelpath): raise NotImplementedError
    def dumpparallel(self, paralleldata): raise NotImplementedError

# unittests
# tests this module's logicic
if __name__ == '__main__':
    import unittest
    import subprocess
    import pandas as pd, numpy as np
    
    class BigDataLogicTest(unittest.TestCase):
        # set testing 
        def setUp(self): 
            # create testing dir
            os.chdir(os.path.split(__file__)[0])
            os.mkdir('tests')
            os.chdir('tests')

            self.mb  = 0.5  # mb (chunksize), reuse
        
            # populate testing data
            origdata = np.random.randn(10 ** 6).reshape((10 ** 5, 10)) # arr
            origdata = pd.DataFrame(origdata)
            # drop data duplicates
            origdata.drop_duplicates(inplace=True)
            # remember original data
            self.origdata = origdata
            # duplicate quarter of data, then ...
            origdata = pd.concat([origdata, 
                                  origdata[:int(len(origdata) / 4)]])  
            # shuffle duplicated data        
            shuffler = np.random.permutation(len(origdata))
            origdata = origdata.take(shuffler)

            # save duplicated data in file (test.json)
            self.origjson = 'test.json'
            origdata.to_json(self.origjson, lines=True, orient='records')

            # define initial data files
            total  = len(origdata)
            third1 = int((1 / 3) * total)
            third2 = int((2 / 3) * total) 
            self.origjsons = {'test1.json': slice(0,      third1, 1),                      
                              'test2.json': slice(third1, third2, 1), 
                              'test3.json': slice(third2, total,  1),}
            # dump data into initial files 
            for fname, third in self.origjsons.items():
                origdata[third].to_json(fname, lines=True, 
                                                orient='records')
                
            # define chunker for reuse
            class ChunkItUp(BigData):
                # runs in constructor (__init__)
                def init(test):
                    # hierarchy root dir
                    chunksdir = 'test'
                    os.mkdir(chunksdir)

                    # chunk initial data files
                    for fname in self.origjsons:
                        pdIO = PandasIO(verbosity=True)
                        data, nchunks, nlines = pdIO.read_json(fname, 
                                                               lines=True,
                                                               mb=True, 
                                                               chunksize=self.mb)
                        
                        # create chunksdir name
                        dirname = os.path.splitext(fname)[0]
                        dirname = os.path.split(dirname)[-1]
                        dirname = os.path.join(chunksdir, dirname)

                        # start chunking
                        test.operate(data, dirname, nchunks)

                        # after successful chunking, save hierarchy root
                        self.chunksdir = chunksdir

                # define event handler
                def onchunkdata(test, data, chunkpath):
                    # dump chunkdata to chunkfile, could do more
                    data.to_json(chunkpath, lines=True, orient='records')        
            # remember chunker
            self.ChunkItUp = ChunkItUp

        # clean testing environment 
        def tearDown(self):
            try:
                for fname in self.origjsons: 
                    os.system('rm %s' % fname)
                os.chdir(os.pardir)
            # tests clean up after themselves
            finally: os.system('rm -r tests') 

        # test BigData and Chunks
        def test_chunking(self):
            # define in-memory loader
            class LoadAll(BigData):
                def init(test): 
                    # remember loaded data
                    test.data = pd.read_json(self.origjson, lines=True)
            
            # implicitly tests BigData
            class CompareChunksToOriginal(Chunks):
                def init(test):
                    # remember chunks
                    test.chunks = []    
                    # output file
                    ofile       = 'test.out'

                    self.ChunkItUp(verbosity=True)            # into chunks
                    loaded  = LoadAll().data                  # load at once
                    test.operate(self.chunksdir, ofile, True) # load in chunks
                    
                    # tests chunking
                    # test data integrity in memory 
                    chunks = pd.concat(test.chunks, ignore_index=True)
                                               # (chunked, original) data
                    index = pd.Index(np.arange(len(loaded)))
                    chunks.index = index
                    loaded.index = index
                    self.assertTrue(loaded.equals(chunks))

                    # tests joining
                    # test data integrity in filesystem  
                    out = subprocess.run(['diff', self.origjson, ofile],
                                        capture_output=True)
                    self.assertEquals(len(out.stdout), 0)

                    # tests cleaning
                    # clear runtime environment
                    self.assertFalse(os.path.exists(self.chunksdir))

                # define event handler
                def onchunkpath(test, chunkpath):
                    test.chunks.append(pd.read_json(chunkpath, lines=True))
            # run test
            CompareChunksToOriginal(verbosity=True)

        # test instance.StopOperation
        def test_operation_stop(self):
            # define sampler
            class IsSampleGood(SamplePd):
                def __init__(         test, min, max, *pargs, **kwargs):
                    # run interface, then ...
                    SamplePd.__init__(test, min, max, *pargs, **kwargs)
                    # assert values
                    self.assertGreaterEqual(len(test.sample), min) # no pd.nan
                    self.assertTrue(test.sample.all().all())       # good sample?
                
                # runs in the constructor above
                def init(test):
                    tempdir = 'temp' # adhere to bigdata protocol (chunksdir)
                    pdIO = PandasIO(verbosity=True)
                    data, nchunks, nlines = pdIO.read_json(self.origjson, 
                                                           lines=True, 
                                                            mb=True,
                                                            chunksize=self.mb)
                    # invoked here for simplicity
                    test.operate(data, tempdir, nchunks, clean=True)
            # run test
            IsSampleGood(5, 10, verbose=2)

        # test ParallelOnce and ParallelRepeat
        def test_parallelization(self):
            # define parallelizer
            class Parallelize(Drop_DuplicatesPd):
                def init(test):
                    # output file
                    ofile = 'test.out'

                    # chunk initial shufled duplicated data file
                    self.ChunkItUp(verbosity=True)

                    # drop duplicates in parallel
                    test.operate(self.chunksdir, ofile, True)

                    # define in-memory loader 
                    class LoadAll(BigData):
                        def init(test):
                            # remember data
                            test.data = pd.read_json(ofile, lines=True)
                    # load initial unduplicated data
                    unique = LoadAll().data

                    # assert values      (original,      operated)
                    self.assertCountEqual(self.origdata, unique)
                
                ##################################################
                # adhere to parallel protocol ...
                ##################################################
                # selfpath 
                def loadself(test, selfpath):
                    # remember selfpath and return data
                    test.selfpath = selfpath
                    return pd.read_json(selfpath, lines=True)
                def dumpself(test, selfdata): 
                    # reuse saved selfpath
                    selfdata.to_json(test.selfpath, lines=True, orient='records')

                # parallelpath
                def loadparallel(test, parallelpath):
                    # remember parallelpath and return data
                    test.parallelpath = parallelpath
                    return pd.read_json(parallelpath, lines=True)
                def dumpparallel(test, paralleldata):
                    # reuse parallelpath
                    paralleldata.to_json(test.parallelpath, lines=True, 
                                                            orient='records')
            # run test
            Parallelize(verbosity=1)
    unittest.main()
