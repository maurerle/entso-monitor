#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 28 09:30:46 2020

@author: maurer
"""
from datetime import *

class Filter:
    def __init__(self, begin: datetime, end: datetime, groupby='day'):
        self.begin = begin
        self.end = end
        self.groupby = groupby

class EntsoeDataManager:
    '''
    Assumptions on the DataManager:
    
    index is time series, everything else are values
    no date and month columns
    '''
    def capacity(self, country: str):
        pass
    
    def load(self, country: str, filt: Filter):
        pass
    
    def generation(self, country: str, filt: Filter):
        pass
    
    def pumpStorage(self, country: str, filt: Filter):
        pass
    
    def crossborderFlows(self, country: str, filt: Filter):
        pass
    
    def countries(self):
        pass
    
import findspark


from pyspark import SparkConf
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import date_trunc

def revReplaceStr(string):
    '''
    replaces series names for better visualization
    '''
    st = str.replace(string,'sum(','')
    st = str.replace(st,')','')
    st = str.replace(st,'(','')
    st = str.replace(st,'_',' ')
    st = st.strip()
    
    return st
    
class EntsoeParquet(EntsoeDataManager):
    def __init__(self, folder, spark):
        self.folder=folder
        
        self.spark = spark

        #load= self.spark.read.parquet("entsoe/DE/query_load")
        
    def capacity(self, country: str):
        cap = self.spark.read.parquet('{}/{}/query_installed_generation_capacity'.format(self.folder,country))
        df = cap.withColumn("time", date_trunc('year',"time")).distinct().sort('time').toPandas()
        df.index=df['time']
        del df['time']
        del df['year']
        del df['month']
        del df['day']
        df.columns = df.columns.map(revReplaceStr)
        return df

    def load(self, country: str, filt: Filter):
        load = self.spark.read.parquet('{}/{}/query_load'.format(self.folder,country))
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        return (load.filter(timeString)
                     .withColumn("group", date_trunc(filt.groupby,"time"))
                     .groupby(['group']).sum()
                     .sort("group").toPandas()
               )
    
    def generation(self, country: str, filt: Filter):
        gen = self.spark.read.parquet('{}/{}/query_generation'.format(self.folder,country))
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        genPandas = (gen.filter(timeString)
                     .withColumn("group", date_trunc(filt.groupby,"time"))
                     .groupby(['group']).sum()
                     .sort("group").toPandas()
               )
        #convert multiindex to index
        genPandas.columns = genPandas.columns.map(''.join).map(revReplaceStr)
        genPandas.index=genPandas['group']
        del genPandas['group']
        del genPandas['year']
        del genPandas['day']
        del genPandas['month']
        # gen = genPandas.melt(id_vars=['time'],  var_name='kind', value_name='value')
        
        
        return genPandas
    
    def pumpStorage(self, country: str, filt: Filter):
        pumpSto = self.spark.read.parquet('{}/{}/query_generation'.format(self.folder,country))
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        return (pumpSto.filter(timeString)
                     .withColumn("group", date_trunc(filt.groupby,"time"))
                     .groupby(['group']).sum()
                     .sort("group").toPandas()
               )
    
    def crossborderFlows(self, country: str, filt: Filter):
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        crossborder= self.spark.read.parquet('{}/query_crossborder_flows'.format(self.folder))
        crossborder=crossborder.filter(timeString).withColumn("group", date_trunc(filt.groupby,"time"))
        
        relList= map(lambda x: x.split('.'),crossborder.columns)
        filteredRelations=filter(lambda x: x.count(country)>0,relList)
        columns=list(map(lambda x: '{}.{}'.format(x[0],x[1]), filteredRelations))       
        columns.append('group')
        
        return crossborder.select(columns).groupby(['group']).sum().toPandas()
    
    def countries(self):
        return ['DE','BE','FR','IT','FI','NL','AT']
    
if __name__ == "__main__":  
    findspark.init()
    conf = SparkConf().setAppName('entsoe').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    par = EntsoeParquet('entsoe',spark)
    cap = par.capacity('FR')
    
    load = par.load('DE', Filter(datetime(2019,1,1),datetime(2019,1,2),'hour'))
    generation = par.generation('DE', Filter(datetime(2019,1,1),datetime(2019,1,2),'hour'))
    gen = generation.melt(var_name='kind', value_name='value',ignore_index=False)
    
    
    begin = datetime(2019,1,1)
    begin.strftime("%Y%m%d")
    
    from entsoe_data_manager import EntsoeDataManager
    issubclass(par.__class__,EntsoeDataManager)
    