#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 12:04:45 2020

@author: maurer
"""

import findspark

findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pandas as pd
import time
import datetime
from pyspark.sql.functions import year, month, to_timestamp

from entsoe import EntsoePandasClient
from helper import persistEntsoe,getDataFrame,yieldData,pullCrossboarders
from tqdm import tqdm

if __name__ == "__main__":  
    
    # Create a spark session
    conf = SparkConf().setAppName('entsoe').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print('')

    entsoe=True
    entsog=True
    if entsoe:
        print()
        print('ENTSOE')
        print()

        client = EntsoePandasClient(api_key='ae2ed060-c25c-4eea-8ae4-007712f95375')
        procs= [client.query_day_ahead_prices,
                client.query_load,
                client.query_load_forecast,
                client.query_generation_forecast,
                client.query_wind_and_solar_forecast,
                client.query_generation,
                client.query_installed_generation_capacity ]
        
        print()
        country_code='DE'
        start = pd.Timestamp('20150101', tz='Europe/Berlin')
        delta=datetime.timedelta(days=90)
        end = start+delta
        
        times=3*4
        countries = ['DE','BE','FR','IT','FI','NL','AT']
        
        persistEntsoe(countries,procs,start,delta,times)
        
        pullCrossboarders(start,delta,times,client.query_crossborder_flows)
    
    if entsog:
        print()
        print('ENTSOG')
        print()
        
        names=[ 'cmpUnsuccessfulRequests',
                   # 'operationaldata',
                    'cmpUnavailables',
                    'cmpAuctions',
                   # 'AggregatedData', # operationaldata aggregated for each zone
                    'tariffssimulations',
                    'tariffsfulls',
                    'urgentmarketmessages',
                    'connectionpoints',
                    'operators',
                    'balancingzones',
                    'operatorpointdirections',
                    'Interconnections',
                    'aggregateInterconnections']
    
    
        t_ges = time.time()
        pbar = tqdm(names)
        for name in pbar:
            try:
                pbar.set_description(name)
                t = time.time()
                data = getDataFrame(name)
                data.to_parquet('entsog/'+name+'.parquet')
                #spark_data = spark.read.parquet(name+'.parquet')
                #print(time.time()-t)
            except Exception as e:
                print(e)
            
        print(time.time()-t_ges)
        
        # unneeded, as we have operationalData
        # name = 'AggregatedData'
        # print('getting values from',name)
        # tt = time.time()
        # for span, phys in tqdm(yieldData(name,bulks=365*3+90,begin=datetime.date(2017,7,10))):
        #     print(span)
        #     spark_data = spark.createDataFrame(phys)
        #     #phys.to_sql(name,conn, if_exists='append')
        #     spark_data.write.mode('append').partitionBy("year","month").parquet('entsog/'+name)        
        #     print('finished',span[0],span[1],time.time()-tt)
        #     tt=time.time()
    
        name = 'operationaldata'
        print('getting values from',name)
        tt = time.time()
        bulks = 365*2+7*30
        begin = datetime.date(2018,4,20)
        for span, phys in tqdm(yieldData(name,bulks=bulks,begin=begin)):
            phys.to_parquet('temp/'+name+'.parquet')
            spark_data = spark.read.parquet('temp/'+name+'.parquet')            
            
            spark_data = (spark_data
                            .withColumn('year', year('periodFrom'))
                            .withColumn('month', month('periodFrom'))
                            # .withColumn("day", dayofmonth("periodFrom"))
                            .withColumn("time", to_timestamp("periodFrom"))
                            # .withColumn("hour", hour("periodFrom"))
                            )
            spark_data.write.mode('append').parquet('entsog/'+name)
            #print('finished',span[0],span[1],time.time()-tt)
            tt=time.time()