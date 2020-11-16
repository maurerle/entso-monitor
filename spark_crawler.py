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
from pyspark.sql.functions import year, month

from entsoe import EntsoePandasClient
from helper import persistEntsoe

if __name__ == "__main__":  
    
    # Create a spark session
    conf = SparkConf().setAppName('entsoe').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print('')

    entsoe=False
    entsog=True
    if entsoe:
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
        
        persistEntsoe(['DE'],procs,start,delta,3*4)
    
    if entsog:
        # ENTSOG
        from entsog_api import yieldData
        
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
    
    
        # t_ges = time.time()
        # for name in names:
        #     try:
        #         print(name)
        #         t = time.time()
        #         data = getDataFrame(name)
        #         data.to_parquet('entsog/'+name+'.parquet')
        #         #spark_data = spark.read.parquet(name+'.parquet')
        #         print(time.time()-t)
        #     except Exception as e:
        #         print(e)
            
        # print(time.time()-t_ges)
        
        # name = 'AggregatedData'
        # print('getting values from',name)
        # tt = time.time()
        # for span, phys in yieldData(name,bulks=365*3+90,begin=datetime.date(2017,7,10))):
        #     print(end1)
        #     spark_data = spark.createDataFrame(phys)
        #     #phys.to_sql(name,conn, if_exists='append')
        #     spark_data.write.mode('append').partitionBy("year","month").parquet('entsog/'+name)        
        #     print('finished',span[0],span[1],time.time()-tt)
        #     tt=time.time()
    
        name = 'operationaldata'
        print('getting values from',name)
        tt = time.time()
        for span, phys in yieldData(name,bulks=365*2+7*30,begin=datetime.date(2018,4,20)):
            phys.to_parquet('entsog/'+name+'.parquet')
            spark_data = spark.read.parquet('entsog/'+name+'.parquet')            
            
            spark_data = (spark_data
                            .withColumn('year', year('periodFrom'))
                            .withColumn('month', month('periodFrom'))
                            # .withColumn("day", dayofmonth("periodFrom"))
                            # .withColumn("time", to_timestamp("periodFrom"))
                            # .withColumn("hour", hour("periodFrom"))
                            )
            spark_data.write.mode('append').partitionBy('year').parquet('entsog/'+name)
            print('finished',span[0],span[1],time.time()-tt)
            tt=time.time()