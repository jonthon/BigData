
# pandas specific interfaces

import pandas as pd

def duplicated(df1, df2):
    df  = pd.concat([df1, df2], keys=['df1', 'df2'])
    dup = df.duplicated()
    return dup.loc['df1'], dup.loc['df2']

def drop_duplicates(df1, df2):
    dup1, dup2 = duplicated(df1, df2)
    df1,  df2  = df1[~dup1], df2[~dup2]
    return df1, df2
