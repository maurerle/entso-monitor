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
            cap = pd.read_sql_query(f'select distinct * from query_installed_generation_capacity where country="{country}"', conn,index_col='index')
        return cap

    def load(self, country: str, filt: Filter):
        whereString=f'country="{country}" and "{filt.begin.strftime("%Y-%m-%d")}" < time and time < "{filt.end.strftime("%Y-%m-%d")}"'
        selectString=f'strftime("{ftime[filt.groupby]}", "index") as time, avg("0") as value'
        groupString=f'strftime("{ftime[filt.groupby]}", "time")'
        with closing(sql.connect(self.database)) as conn:
            query = f"select {selectString} from query_load where {whereString} group by {groupString}"
            load = pd.read_sql_query(query,conn,index_col='time')
        return load
        
            
    def generation(self, country: str, filt: Filter):
        whereString=f'country="{country}" and "{filt.begin.strftime("%Y-%m-%d")}" < time and time < "{filt.end.strftime("%Y-%m-%d")}"'
        selectString=f'strftime("{ftime[filt.groupby]}", "index") as time'
        groupString=f'strftime("{ftime[filt.groupby]}", "time")'
        with closing(sql.connect(self.database)) as conn:
            columns = pd.read_sql_query(f'select * from query_generation where 1=0',conn).columns
            colNames = '","'.join(columns)
            colNames = '"'+colNames+'"'
            query = f"select {selectString},{colNames} from query_generation where {whereString} group by {groupString}"
            gen = pd.read_sql_query(query,conn)
        gen.columns = gen.columns.map(''.join).map(revReplaceStr)
        gen.index = gen['time']
        del gen['index']
        del gen['time']
        return gen
    
    def generated(self, country: str, filt: Filter):
        whereString=f'country="{country}" and "{filt.begin.strftime("%Y-%m-%d")}" < time and time < "{filt.end.strftime("%Y-%m-%d")}"'
        selectString=f'strftime("{ftime[filt.groupby]}", "index") as time'
        groupString=f'strftime("{ftime[filt.groupby]}", "time")'
        with closing(sql.connect(self.database)) as conn:
            columns = pd.read_sql_query(f'select * from query_generation where 1=0',conn).columns
            
            columns= list(filter(lambda x: x.endswith('Actual_Consumption'), map(str,columns)))
            colNames = '","'.join(columns)
            colNames = '"'+colNames+'"'
            query = f"select {selectString},{colNames} from query_generation where {whereString} group by {groupString}"
            gen = pd.read_sql_query(query,conn,index_col='time')
        gen.columns = gen.columns.map(''.join).map(revReplaceStr)
        return gen
    def crossborderFlows(self, country: str, filt: Filter):
        
        # relList= map(lambda x: x.split('.'),crossborder.columns)
        # filteredRelations=filter(lambda x: x.count(country)>0,relList)
        # columns=list(map(lambda x: '{}.{}'.format(x[0],x[1]), filteredRelations))       
        # columns.append('group')
        
        # return crossborder.select(columns).groupby(['group']).sum().toPandas()
        pass
    
    def powersystems(self, country=''):
        selectString='eic_code,p.name,company,p.country,q.country as area,lat,lon,capacity,Production_Type'
        if country=='':
            whereString=''
        else:
            whereString=f'where p.country="{country}"'
        with closing(sql.connect(self.database)) as conn:
            df = pd.read_sql(f'select {selectString} from powersystemdata p join query_installed_generation_capacity_per_unit q on q."index" = p.eic_code {whereString}', conn)    
        return df
    
    def powersystems2(self, country):
        selectString='eic_code,p.name,company,p.country,lat,lon,capacity,Production_Type'
        with closing(sql.connect(self.database)) as conn:
            df = pd.read_sql(f'select {selectString} from powersystemdata p join {country}_query_installed_generation_capacity_per_unit q on q."index" = p.eic_code', conn)    
        return df
    
    def countries(self):
        with closing(sql.connect(self.database)) as conn:
            df = pd.read_sql('select distinct country from query_generation', conn)    
        return list(df['country'])
    
    
if __name__ == "__main__":  
    country='NL'
    par = EntsoeSQLite('entsoe.db')
    cap = par.capacity(country)
    countries= par.countries()
    country = countries[0]
    
    filt = Filter(datetime(2020,2,1),datetime(2020,2,2),'hour')
    load = par.load(country, filt)
    generation = par.generation(country, filt)
    del generation['country']
    generation=generation/1000
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