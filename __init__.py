
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
    # base system interface with mixin interface logic 
    # (ie. self.StopOperation, self.init)
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
    """
    def operate(self, data, chunksdir, nchunks=None, opath=None, clean=False):
        """
        """
        if self.verbosity: print('operating ...')

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
    def operate(self, chunksdir, opath=None, clean=False):
        if self.verbosity: print('operating ...')

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
    # (nchunks ** 2) runs
    def onchunkpath(self, selfpath):
        # starts parallel operation loop ..........................
        for parallelpath in self.chunkspaths:
            # stops parallel operation loop or ....................
            self.onparallel(selfpath, parallelpath)
        # completes parallel operation ............................
    def onparallel(self, selfpath, parallelpath):
        raise NotImplementedError

class ParallelOnce(ParallelRepeat):
    # math.factorial(nchunks) runs
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
class Pandas:
    def __getattr__(self, attr):
        #****************************************************
        # intercepts pandas.read_* functions for file reading
        #****************************************************
        def read_(ipath, *, mb=False, **kwargs):
            "Customizes pandas reader to take chunksize in MB"
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
            "dumps pandas data to file in desired format"
            return getattr(data, attr)(opath, **kwargs)
        
        # calls respective pandas io interface
        if   'read_' in attr: return read_
        elif 'to_'   in attr: return to_
        raise AttributeError(self.__class__.__name__ + 
                             " doesn't have %s attribute" % attr) 

class BigDataPd(        BigData,        Pandas): pass 
class ChunksPd(         Chunks,         Pandas): pass
class ParallelPdRepeat( ParallelRepeat, Pandas): pass
class ParallelPdOnce(   ParallelOnce,   Pandas): pass


#################################################################
# pandas specific interfaces
#################################################################
class SamplePd(BigDataPd):
    "Returns a sample of data according to passed df.dropna args"
    def __init__(self, min, max, *, verbose=False, **kwargs):
        self.min, self.max = min, max
        self.kwargs = kwargs
        self.sample = None
        BigDataPd.__init__(self, verbosity=verbose)
    def onchunkdata(self, data, chunkpath):
        chunk = data.dropna(**self.kwargs)
        if chunk.shape[0] >= self.min: 
            self.onsample(chunk.iloc[:self.max], chunkpath)
            raise self.StopOperation
    def onsample(self, sample, chunkpath): self.sample = sample

class DropDuplicatesPd(ParallelPdOnce):
    def onparallelonce(self, selfpath, parallelpath):
        if selfpath == parallelpath:
            data = self.loadself(selfpath)
            data.drop_duplicates(inplace=True)
            self.dumpself(data)
            self.data = data
            return
        df2      = self.loadparallel(parallelpath)
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
            origdata = origdata.sample(frac=1).reset_index(drop=True)

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
            class ChunkItUp(BigDataPd):
                # runs in constructor (__init__)
                def init(test):
                    # hierarchy root dir
                    chunksdir = 'test'
                    os.mkdir(chunksdir)

                    # chunk initial data files
                    for fname in self.origjsons:
                        data, nchunks, nlines = test.read_json(fname, 
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
                    test.to_json(data, chunkpath, lines=True, 
                                                  orient='records')        
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
            class LoadAll(BigDataPd):
                def init(test): 
                    # remember loaded data
                    test.data = test.read_json(self.origjson, lines=True)
            
            # implicitly tests BigData
            class CompareChunksToOriginal(ChunksPd):
                def init(test):
                    # remember chunks
                    test.chunks = []    
                    # output file
                    ofile       = 'test.out'

                    self.ChunkItUp(verbosity=True)     # into chunks
                    loaded  = LoadAll().data[0]                  # load at once
                    test.operate(self.chunksdir, ofile, True) # load in chunks
                    
                    # tests chunking
                    # test data integrity in memory 
                    chunks = pd.concat(test.chunks, ignore_index=True)
                                               # (chunked, original) data
                    pd.testing.assert_frame_equal(chunks,  loaded)

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
                    data, nchunks, nlines = test.read_json(self.origjson, 
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
            class Parallelize(DropDuplicatesPd):
                def init(test):
                    # output file
                    ofile = 'test.out'

                    # chunk initial shufled duplicated data file
                    self.ChunkItUp(verbosity=True)

                    # drop duplicates in parallel
                    test.operate(self.chunksdir, ofile, True)

                    # define in-memory loader 
                    class LoadAll(BigDataPd):
                        def init(test):
                            # remember data
                            test.data = test.read_json(ofile, lines=True)[0]
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
                    return test.read_json(selfpath, lines=True)[0]
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
            Parallelize(verbosity=3)
    unittest.main()
