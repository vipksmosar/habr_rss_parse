import os
try:
    import pandas as pd
except:
    os.system('pip3 install pandas')
    import pandas as pd
try:
    import psycopg2
except:
    os.system('pip3 install psycopg2-binary')
    import psycopg2
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

class POSTGREE_WRITER:
    
    def __init__(self, string_to_connect='postgresql://postgres:postgres@pdbserv:5432/postgres'):
        
        self.string_to_connect = string_to_connect
        self.conn = psycopg2.connect(self.string_to_connect)
        self.cursor = self.conn.cursor()
        
    def __create_DF(self, path='./habr_news/'):
        DF_ALL = pd.DataFrame()
        for file in os.listdir(path):
            full_filename = '{}{}'.format(path, file)
            DF = pd.read_parquet(full_filename)
            DF_ALL = pd.concat([DF_ALL, DF])
        DF_ALL = DF_ALL.drop_duplicates()
        return DF_ALL
    
    def __clear_directory(self, path='./habr_news/'):
        for file in os.listdir(path):
            full_filename = '{}{}'.format(path, file)
            os.remove(full_filename)
        
    
    def sql_test(self, table_name):
        select_req = '''SELECT * FROM {};'''.format(table_name)
        #select_req = '''SELECT * FROM INFORMATION_SCHEMA.TABLES'''
        self.cursor.execute(select_req)
        data_final = self.cursor.fetchall()
        columns_data = list(map(lambda x: x[0], self.cursor.description))
        self.conn.commit()
        return data_final, columns_data
    
    def sql_insert(self, table_name, data):
        self.__init__('postgresql://postgres:postgres@pdbserv:5432/postgres')
        select_insert = '''INSERT INTO {}
        (
        title,
        link,
        author_name,
        real_author_name,
        body,
        index_hash,
        create_at,
        text_len,
        word_count,
        word_mean,
        eng_symbol ,
        rus_symbol,
        num_symbol 
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        '''.format(table_name)
        self.cursor.execute(select_insert, data)
        self.conn.commit()
    
    def sql_start(self, path):
        self.result = {'error_count':0, 'success_count':0}
        self.create_table_if_not_exist('habr_news')
        data = self.__create_DF(path)
        for i in range(len(data)):
            try:
                string = data.iloc[i]
                self.sql_insert('habr_news',(string['title'], string['link'], string['author_name'],
                                          string['real_author_name'],string['body'], string['index_hash'],
                                          string['create_at'], string['text_len'], string['word_count'],
                                          string['word_mean'], string['eng_symbol'], string['rus_symbol'],
                                          string['num_symbol']))
                self.result['success_count']+=1
            except Exception as E:
                #print(E)
                if 'duplicate key value violates unique constraint' in '{}'.format(E):
                    self.result['error_count']+=1
        #data, columns = self.sql_test('habr_news')
        self.cursor.close()
        self.conn.close()
        if self.result['error_count']==len(data):
            self.__clear_directory('/opt/airflow/tmp_dir/habr_news/')
        return self.result
    
    def create_table_if_not_exist(self, table_name):
        select_insert = '''create table if not exists {} (
        title TEXT,
        link TEXT,
        author_name TEXT,
        real_author_name TEXT,
        body TEXT,
        index_hash VARCHAR NOT NULL,
        create_at TIMESTAMP,
        text_len INT,
        word_count REAL,
        word_mean REAL,
        eng_symbol INT,
        rus_symbol INT,
        num_symbol INT,
        CONSTRAINT habr_news_pk PRIMARY KEY (index_hash)
        )'''.format(table_name)
        self.cursor.execute(select_insert)
        self.conn.commit()