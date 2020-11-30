#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 21:55:39 2020

@author: maurer
"""

import findspark

from entsoe_data_manager import EntsoeDataManager, Filter, revReplaceStr
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_trunc

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
        data = (load.filter(timeString)
                     .withColumn("time", date_trunc(filt.groupby,"time"))
                     .groupby(['time']).sum()
                     .sort("time").toPandas()
               )
        data['value'] = data['sum(0)']
        del data['sum(0)']
        
        return data
    
    def generation(self, country: str, filt: Filter):
        gen = self.spark.read.parquet('{}/{}/query_generation'.format(self.folder,country))
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        cols = list(filter(lambda x: x.endswith('_Actual_Aggregated') ,gen.columns))        
        cols.append('time')
        gen = gen.select(cols)
        newcols=list(map(lambda x: x.replace('_Actual_Aggregated',''), gen.columns))
        gen = gen.toDF(*newcols)
        genPandas = (gen.filter(timeString)
                     .withColumn("time", date_trunc(filt.groupby,"time"))
                     .groupby(['time']).sum()
                     .sort("time").toPandas()
               )
        
        #convert multiindex to index, and make columns readable
        genPandas.columns = genPandas.columns.map(''.join).map(revReplaceStr)
        genPandas.index=genPandas['time']
        del genPandas['time']
        # gen = genPandas.melt(id_vars=['time'],  var_name='kind', value_name='value')
        
        
        return genPandas
    
    def consumption(self, country: str, filt: Filter):
        pumpSto = self.spark.read.parquet('{}/{}/query_generation'.format(self.folder,country))
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        cols = list(filter(lambda x: x.endswith('Actual_Consumption') ,pumpSto.columns))
        pumpSto = pumpSto.select(cols)
        data= (pumpSto.filter(timeString)
                     .withColumn("time", date_trunc(filt.groupby,"time"))
                     .groupby(['time']).sum()
                     .sort("time").toPandas()
               )
        data.columns = data.columns.map(''.join).map(revReplaceStr)
        data.index = data['time']
        del data['time']
        return data
    
    def crossborderFlows(self, country: str, filt: Filter):
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        crossborder= self.spark.read.parquet('{}/query_crossborder_flows'.format(self.folder))
        crossborder=crossborder.filter(timeString).withColumn("group", date_trunc(filt.groupby,"time"))
        
        relList= map(lambda x: x.split('.'),crossborder.columns)
        filteredRelations=filter(lambda x: x.count(country)>0,relList)
        columns=list(map(lambda x: '{}.{}'.format(x[0],x[1]), filteredRelations))       
        columns.append('group')
        
        return crossborder.select(columns).groupby(['group']).sum().toPandas()
    
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