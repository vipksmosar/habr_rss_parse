import os
import pandas as pd
import numpy as np
from datetime import datetime
import ast
try:
    from tqdm import tqdm
except:
    os.system('pip3 install tqdm')
    from tqdm import tqdm
import hashlib
import re

class DATA_PREPARE_TRANSFORM:
    
    def __init__(self, dict_to):
        self.dict_to = dict_to
    
    def prepare(self):
        tqdm.pandas()
        DF = pd.DataFrame(self.dict_to).T
        DF['prep_hash']=DF.title + DF.link + DF.published
        DF['index_hash']=DF['prep_hash'].progress_apply(lambda x: hashlib.md5(bytes(x, encoding="raw_unicode_escape")).hexdigest())
        DF['create_at'] = pd.to_datetime(DF.published)
        DF['text_len']=DF.body.progress_apply(len)
        DF['word_count']=DF.body.progress_apply(lambda x:len(x.split(' ')))
        DF['word_mean']=DF.body.progress_apply(lambda x:round(np.mean(list(map(len,x.split(' '))))))
        DF['eng_symbol']=DF.body.progress_apply(lambda x:len(re.findall('[A-z]', x)))
        DF['rus_symbol']=DF.body.progress_apply(lambda x:len(re.findall('[А-я]', x)))
        DF['num_symbol']=DF.body.progress_apply(lambda x:len(re.findall('\d', x)))
        DF = DF[['title', 'link', 'published', 'author_name', 'real_author_name', 'body',
        'index_hash', 'create_at', 'text_len', 'word_count',
       'word_mean', 'eng_symbol', 'rus_symbol', 'num_symbol']]
        
        return DF
    
    def write_to_file(self, file_name):
        DF = self.prepare()
        DF.to_parquet('{}_{}.parquet'.format(file_name, datetime.now().strftime('%d_%m_%Y_%H_%M')))