#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 23:26:05 2020

@author: maurer
"""

import pandas as pd
import sqlite3
from contextlib import closing



def pd2par(dbname,table,parquet=''):
    '''
    converts sqlite database table to parquet file
    '''
    if parquet =='':
        parquet=table+'.parquet'
    with closing(sqlite3.connect(dbname)) as conn:
        df = pd.read_sql_query('select * from '+table, conn)
        df.to_parquet(parquet)

def par2pd(dbname,parquet,table=''):
    '''
    converts parquet file to sqlite database table
    '''
    if table =='':
        table=parquet
    with closing(sqlite3.connect(dbname)) as conn:
        df = pd.read_parquet(parquet)
        df.to_sql(table, conn)
        
def prerror():
    '''
    prints last thrown error
    '''
    import traceback,sys
    traceback.print_exception(etype=sys.last_type,value=sys.last_value,tb=sys.last_traceback)