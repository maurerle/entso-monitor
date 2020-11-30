#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 14:58:52 2020

@author: maurer
"""

from entsoe_data_manager import EntsoeDataManager, Filter, revReplaceStr

import sqlite3 as sql
from contextlib import closing

from datetime import datetime, date
import pandas as pd

ftime={'day': '%Y-%m-%d',
       'month': '%Y-%m-01',
       'year': '%Y-01-01',
       'hour': '%Y-%m-%d %H:00:00',
       'minute': '%Y-%m-%d %H:%M:00'}

class EntsoeSQLite(EntsoeDataManager):
    def __init__(self, database: str):
        self.database=database
        
    def capacity(self, country: str):
        with closing(sql.connect(self.database)) as conn:
            cap = pd.read_sql_query('select * from {}_query_installed_generation_capacity'.format(country), conn)
        
        cap.index = cap.time
        del cap['time']
        del cap['index']
        return cap

    def load(self, country: str, filt: Filter):
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        selectString=f'strftime("{ftime[filt.groupby]}", "index") as time, avg("0") as value'
        with closing(sql.connect(self.database)) as conn:
            query = f"select {selectString} from {country}_query_load where {timeString} group by time"
            load = pd.read_sql_query(query,conn)
        load.index = load['time']
        del load['time']
        return load
        
            
    def generation(self, country: str, filt: Filter):
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        selectString=f'strftime("{ftime[filt.groupby]}", "index") as time'
        
        with closing(sql.connect(self.database)) as conn:
            columns = pd.read_sql_query(f'select * from {country}_query_generation where 1=0',conn).columns
            columns= list(filter(lambda x: x.endswith('Actual_Aggregated'), map(str,columns)))
            colNames = '","'.join(columns)
            colNames = '"'+colNames+'"'
            query = f"select {selectString},{colNames} from {country}_query_generation where {timeString} group by time"
            gen = pd.read_sql_query(query,conn)
        gen.columns = gen.columns.map(''.join).map(revReplaceStr)
        gen.index = gen['time']
        del gen['time']
        return gen
    
    def pumpStorage(self, country: str, filt: Filter):
        pass
    
    def crossborderFlows(self, country: str, filt: Filter):
        
        # relList= map(lambda x: x.split('.'),crossborder.columns)
        # filteredRelations=filter(lambda x: x.count(country)>0,relList)
        # columns=list(map(lambda x: '{}.{}'.format(x[0],x[1]), filteredRelations))       
        # columns.append('group')
        
        # return crossborder.select(columns).groupby(['group']).sum().toPandas()
        pass

if __name__ == "__main__":  

    par = EntsoeSQLite('data/entsoe2.db')
    cap = par.capacity('DE')
    
    filt = Filter(datetime(2015,2,1),datetime(2015,2,2),'hour')
    load = par.load('DE', filt)
    generation = par.generation('DE', filt)
    gen = generation.melt(var_name='kind', value_name='value',ignore_index=False)
    
    # from entsoe_data_manager import EntsoeDataManager
    # issubclass(par.__class__,EntsoeDataManager)

    # with closing(sql.connect('entsoe.db')) as conn:
    #     data = pd.read_sql_query(f'select "index" from query_crossborder_flows',conn)
    # with closing(sql.connect('data/entsoe2.db')) as conn:         
    #     data.to_sql('query_crossborder_flows',conn)
    #     columns = pd.read_sql_query(f'select * from DE_query_generation where 1=0',conn).columns
    #         query = "select * from DE_query_generation"
    #         gen = pd.read_sql_query(query,conn)