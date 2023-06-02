
import pandas as pd
import subprocess, os

def bson_to_json(bfile, jfile=None, type='json'):
    "Exports bson file to json. jfile=bfile.type by default"
    cmd = 'bsondump --bsonFile=%s --outFile=%s --type=%s'
    cmd = cmd % (bfile, jfile, type,).split()
    if not jfile: jfile = os.path.splitext(bfile)[0] + '.' + type
    subprocess.run()    # catch errors?
    return jfile

def to_frame(docs, columns=None):
    """
    Returns a frame with keys as columns for single docs or a frame 
    with indices as columns for nested docs.
    """
    if not columns: docs = docs.dropna()            # single
    def to_series(doc):
        row = pd.Series(doc, dtype=object)
        if row.empty:                               # nested
            row = pd.Series([None] * len(columns), dtype=object)
        return row
    frame = docs.apply(to_series)
    if columns: frame.columns = columns             # rename cols
    return frame

def to_values(docs, key, ignore_na=True):
    """
    Returns a series with keys as indices for single docs or a series 
    with positional indices (ie. int) as indices for nested docs.
    """
    def to_value(doc):
        return doc[key]
    kwargs = dict()
    if ignore_na: kwargs.update({'na_action': 'ignore'})
    return docs.map(to_value, **kwargs)

