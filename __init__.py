
import pandas as pd
import os

# don't relative import when unittesting or error
if __name__ != '__main__':
    from . import (_jsonmgr as json, _pdmgr as pdmgr,)
# modules from this package used in unittesting
else:
    from datamgr import _pdmgr as pdmgr

#################################################################
# System interfaces
#################################################################
class BigData:
    "Customize, do not use directly. See BigDataPd"

    class StopOperation(Exception): pass

    def __init__(self, *, verbose=False):
        # assign attributes for use in higher classes for 
        # explicitness
        self.verbose     = verbose
        self.data        = None
        self.fpath       = ''
        self.chunksdir   = ''
        self.chunkspaths = ''
        # run operation during construction for simplicity
        self.init()
    
    def init(self): pass

    def operate(self, chunksdir=None, opath=None, clean=False):
        """
        Calls self.ondata on every loaded file data chunk. Throws 
        exception if chunksdir exists. 
        """
        if self.verbose: print('operating ...')

        #**********************************************
        # operation handler logic, customize as desired
        #**********************************************
        # make chunksdir if not yet
        temp, _ = os.path.splitext(self.fpath)
        temp    = temp.split(os.sep)[-1]
        if not chunksdir: chunksdir = temp; del temp
        os.mkdir(chunksdir)           # check for existence?
        self.chunksdir = chunksdir    # after successful os.makdir
                                      # (save state)

        # nrows data library interface arg (i.e pandas.read_*)
        if not self.chunksize:        # prioritize chunks to rows
            chunkpath = os.path.join(chunksdir, chunksdir) 
            self.ondata(self.data, chunkpath)
            if self.verbose: print('done!')
            return
        
        # chunksize data library interface arg (i.e pandas.read_*)
        chunkspaths = []
        try:
        # starts operation loop ..........................................
            for chunk, data in enumerate(self.data, 1):
                chunkpath = str(len(str(self.nchunks))) # digits precision
                chunkpath = chunksdir + '-%0' + chunkpath + 'd'
                chunkpath = os.path.join(chunksdir, (chunkpath % chunk))
                chunkspaths.append(chunkpath)
                self.chunkspaths = chunkspaths # for higher classes (state)
                # operate on every chunk
                # stops operation loop or ................................
                self.ondata(data, chunkpath)
        # completes operation loop .......................................
        except self.StopOperation: pass

        #**************************************************
        # Joining occurs here. Keep the protocol or emulate
        #**************************************************
        if opath: 
            self.joinchunks(chunkspaths, opath, clean=clean, 
                                                chunksdir=chunksdir)
        if self.verbose: print('done!')

    def joinchunks(self, chunkspaths, opath, *, clean=False,     # pass
                                                chunksdir=None): # both
        "joins chunkfiles to one file"
        if self.verbose: print('joining   ...')
        # leave if there are no chunks (save system resources)
        file = open(opath, 'wb') 
        chunkspaths.sort()
        for chunkpath in chunkspaths:
            file.write(open(chunkpath, 'rb').read())
        file.close()
        # clean
        if clean and chunksdir: self.clean(chunksdir)

    def clean(self, chunksdir):
        # removes directory recursively
        if self.verbose: print('cleaning  ...')
        os.system('rm -r %s' % chunksdir)

    def ondata(self, data, chunkpath): raise NotImplementedError
    
class Chunks(BigData):
    class StopOperation(Exception): pass
    def operate(self, chunksdir, opath=None, clean=False):
        # load paths instead of splitting file, see BigDataPd
        if self.verbose: print('operating ...')
        chunkspaths = os.listdir(chunksdir)
        chunkspaths.sort()             # corrupts data if not called 
        chunkspaths = [os.path.join(chunksdir, chunkpath) 
                       for chunkpath in chunkspaths]
        self.chunksdir   = chunksdir   # for use in higher classes (state)
        self.chunkspaths = chunkspaths # for use in higher classes (state)

        # starts operation loop ....................
        try:
            # stops operation loop or ..............
            list(map(self.ondata, self.chunkspaths))
        # completes operation loop .................
        except self.StopOperation: pass

        # emulates joining invocation
        if opath: self.joinchunks(chunkspaths, opath, clean=clean, 
                                                chunksdir=chunksdir)
        if self.verbose: print('done!')
    def ondata(self, chunkpath): raise NotImplementedError

class ParallelRepeat(Chunks):
    def ondata(self, selfpath):
        # starts parallel operation loop ..........................
        for parallelpath in self.chunkspaths:
            # stops parallel operation loop or ....................
            self.onparallel(selfpath, parallelpath)
        # completes parallel operation ............................
    def onparallel(self, selfpath, parallelpath):
        raise NotImplementedError
    def loadself(    self, selfpath    ): raise NotImplementedError
    def dumpself(    self, selfdata    ): raise NotImplementedError    
    def loadparallel(self, parallelpath): raise NotImplementedError
    def dumpparallel(self, paralleldata): raise NotImplementedError

class ParallelOnce(ParallelRepeat):
    def __init__(self, *pargs, **kwargs):
        self.skip = ''
        ParallelRepeat.__init__(self, *pargs, **kwargs)
    def onparallel(self, selfpath, parallelpath):
        if not parallelpath > self.skip: return
        self.onparallelonce(selfpath, parallelpath)
        self.skip = selfpath
    def onparallelonce(self, selfpath, parallelpath): 
        raise NotImplementedError


################################################################
# pandas system interfaces, they customize BigData, Chunks, 
# ParallelRepeat, and ParallelOnce. 
################################################################
# pandas mixin
class Pandas:
    def __getattr__(self, attr):
        #****************************************************
        # intercepts pandas.read_* functions for file reading
        #****************************************************
        def read(fpath, **kwargs):
            "Customizes pandas reader to take chunksize in MB"
            def chunk_mb_to_lines(ipath, chunksize):
                "Converts chunksize from MB to nlines"
                if self.verbose: print('counting  ...')
                import math
                import subprocess
                # file lines and size counts
                temp = subprocess.run(['wc', '-lc', ipath], 
                                    capture_output=True)
                flines, fsize = temp.stdout.decode().split()[:2]; del temp
                flines, fsize = int(flines), int(fsize)
                nlines = math.ceil(chunksize * (10 ** 6) * flines / fsize)
                # return (chunklines, nchunks) tuple 
                return nlines, math.ceil(flines / nlines) 
            
            # emulate pd chunksize protocol and keep the rest
            try:
                if kwargs['chunksize']: self.chunksize = kwargs['chunksize']
                else:                   raise KeyError
                self.chunklines, self.nchunks = chunk_mb_to_lines(
                                                        fpath, 
                                                        self.chunksize)
                kwargs['chunksize'] = self.chunklines
            # keep pandas' defaults
            except KeyError: self.chunksize = None

            if self.verbose: print('reading   ...')
            self.fpath = fpath
            self.data  = getattr(pd, attr)(fpath, **kwargs)
        
        #**************************************************
        # intercepts pandas.to_* write methods
        #**************************************************
        def write(data, opath, **kwargs):
            "dumps pandas data to file in desired format"
            return getattr(data, attr)(opath, **kwargs)
        
        # calls respective pandas io interface
        if   'read_' in attr: return read
        elif 'to_'   in attr: return write
        raise AttributeError(self.__class__.__name__ + 
                             " doesn't have %s attribute" % attr) 
    
class BigDataPd(        Pandas, BigData       ): pass
class ChunksPd(         Pandas, Chunks        ): pass
class ParallelPdRepeat( Pandas, ParallelRepeat): pass
class ParallelPdOnce(   Pandas, ParallelOnce  ): pass


#################################################################
# pandas data specific interfaces
#################################################################
class SamplePd(BigDataPd):
    "Returns a sample of data according to passed df.dropna args"
    def __init__(self, min, max, *, verbose=False, **kwargs):
        self.min, self.max = min, max
        self.kwargs = kwargs
        self.sample = None
        BigDataPd.__init__(self, verbose=verbose)                     # operate
        if os.path.exists(self.chunksdir): self.clean(self.chunksdir) # clean up
        return self.sample            # for simplicity return sample in one shot
    def ondata(self, data, chunkpath):
        chunk = data.dropna(**self.kwargs)
        if chunk.shape[0] >= self.min: 
            self.onsample(chunk.iloc[:self.max], 
                                        chunkpath)
            # stops here, after getting a desired sample
            raise self.StopOperation
    def onsample(self, sample, chunkpath): self.sample = sample

class DropDuplicatesPd(ParallelPdOnce):
    def onparallelonce(self, selfpath, parallelpath):
        if selfpath == parallelpath:
            self.loadself(selfpath)
            self.data.drop_duplicates(inplace=True)
            self.dumpself(self.data)
            return
        df2      = self.loadparallel(parallelpath)
        ign, df2 = pdmgr.drop_duplicates(self.data, df2)
        self.dumpparallel(df2)

# unittests
# tests this module's logicic
if __name__ == '__main__':
    import unittest
    import subprocess
    import pandas as pd, numpy as np
    
    class BigDataLogicTest(unittest.TestCase):
        # create testing files, atleast 25 MB each
        def setUp(self):
            self.origjson  = 'test.json'
            self.chunksdir = 'test'
            self.mb        = 5        # mb => 50 chunks
            origdata = np.random.randn(10 ** 6).reshape((10 ** 5, 10)) # arr
            origdata = pd.DataFrame(origdata)
            # drop duplicates
            origdata.drop_duplicates(inplace=True)
            self.origdata = origdata
            # duplicate quarter of data and shuffle
            origdata = pd.concat([origdata, 
                                  origdata[:int(len(origdata) / 4)]])          
            origdata = origdata.sample(frac=1).reset_index(drop=True)
            os.chdir(os.path.split(__file__)[0])
            os.mkdir('tests')
            os.chdir('tests')
            origdata.to_json(self.origjson, lines=True, orient='records')

        # remove testing dirs and files
        def tearDown(self):
            try:
                os.system('rm %s' % self.origjson)
                os.chdir(os.pardir)
            # tests clean up after themselves
            finally: os.system('rm -r tests') 

        # main logic test
        def test_bigdata_logic(self):
            class ChunkItUp(BigDataPd):
                def init(test):
                    test.read_json(self.origjson, lines=True, 
                                                  chunksize=self.mb)
                    # default args
                    test.operate() 
                def ondata(test, data, chunkpath):
                    # actual custom classes do more here
                    test.to_json(data, chunkpath, lines=True, 
                                                  orient='records')
            class LoadAll(BigDataPd):
                # loads all data in memory
                def init(test): test.read_json(self.origjson, lines=True)
            
            # internally tests BigDataPd
            class CompareChunksToOriginal(ChunksPd):
                def init(test):
                    test.chunks = []    
                    ofile       = 'test.out'
                    ChunkItUp()                               # chop into chunks
                    loaded = LoadAll().data                   # load at once
                    test.operate(self.chunksdir, ofile, True) # load in chunks
                    # test data integrity in memory 
                    # tests chunking
                    chunks = pd.concat(test.chunks, ignore_index=True)
                    pd.testing.assert_frame_equal(chunks, loaded)
                    # test data integrity in filesystem 
                    # tests joining 
                    out = subprocess.run(['diff', self.origjson, ofile],
                                        capture_output=True)
                    self.assertEquals(len(out.stdout), 0)
                    # clears runtime environment
                    # test cleaning
                    self.assertFalse(os.path.exists(self.chunksdir))
                # bundle chunks
                def ondata(test, chunkpath):
                    test.chunks.append(pd.read_json(chunkpath, lines=True))
            
            # test if the operation loop stops on demand
            class IsSampleGood(SamplePd):
                def __init__(         test, min, max, *pargs, **kwargs):
                    SamplePd.__init__(test, min, max, *pargs, **kwargs)
                    # it has to have a sample in this test
                    # tests BigData.StopOperation
                    self.assertGreaterEqual(len(test.sample), min)  # no pd.nan
                    self.assertTrue(test.sample.all().all())        # good sample?
                def init(test):
                    test.read_json(self.origjson, lines=True, 
                                                  chunksize=self.mb)
                    test.operate()
            
            # tests parallels
            class Parallelize(DropDuplicatesPd):
                def init(test):
                    ofile = 'test.out'
                    # chunk duplicated and shuffled file
                    ChunkItUp()
                    # drop duplicates in chunks
                    test.operate(self.chunksdir, ofile, True)
                    # define loader and load
                    class LoadAll(BigDataPd):
                        # loads all data in memory
                        def init(test): test.read_json(ofile, lines=True)
                    unique = LoadAll().data
                    # assert equality
                    self.assertCountEqual(self.origdata, unique)
                def loadself(test, selfpath): 
                    test.read_json(selfpath, lines=True)
                def dumpself(test, selfdata): 
                    selfdata.to_json(test.fpath, lines=True, 
                                                 orient='records')
                def loadparallel(test, parallelpath):
                    test.parallelpath = parallelpath
                    return pd.read_json(parallelpath, lines=True)
                def dumpparallel(test, paralleldata):
                    paralleldata.to_json(test.parallelpath, 
                                         lines=True, 
                                         orient='records')
            # tests are invoked here, but defined on top
            CompareChunksToOriginal()
            IsSampleGood(5, 10)
            Parallelize()
    unittest.main()
