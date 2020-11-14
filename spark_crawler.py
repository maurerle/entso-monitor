#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 12:04:45 2020

@author: maurer
"""

import findspark
import requests
import time
import datetime
from entsoe import EntsoePandasClient
from entsoe.mappings import PSRTYPE_MAPPINGS,NEIGHBOURS,Area
from entsoe.exceptions import NoMatchingDataError

findspark.init()


import pyspark
from pyspark.sql import SparkSession
import pandas as pd

# Create a spark session
spark = SparkSession.builder.getOrCreate()

# ENTSOE

from_n = []
to_n = []
for n1 in NEIGHBOURS:
    for n2 in NEIGHBOURS[n1]:
        from_n.append(n1)
        to_n.append(n2)
neighbours=pd.DataFrame({'from':from_n,'to':to_n})


psrtype =pd.DataFrame.from_dict(PSRTYPE_MAPPINGS,orient='index')
areas = pd.DataFrame([[e.name,e.value,e._tz,e._meaning] for e in Area])

from pyspark.sql.functions import year, month, dayofmonth, to_timestamp, hour, to_date

def pullData(procedure,country_code, start,end):
    t = time.time()
    data = pd.DataFrame(procedure(country_code,start=start,end=end))
    data['time'] = data.index
    spark_data = spark.createDataFrame(data)
    dur = time.time()-t
    print(procedure.__name__, dur)
    return spark_data

def replaceStr(string):
    
    st = str.replace(string,')','')
    st = str.replace(st,'(','')
    st = str.replace(st,',','')
    st = str.replace(st,"'",'')
    st = st.strip()
    st = str.replace(st,' ','_')
    return st

def entsoeToParquet(countries,procs,start,delta,times):
    end = start+delta-datetime.timedelta(days=1)
    for country_code in countries:
        for i in range(times):
            start = start + delta
            end = end + delta
            print(country_code, start, end)
            print('')
            for proc in procs:
                try:
                    spark_data = pullData(proc,country_code, start, end)    
                    # replace spaces in column names
                    new_names = list(map(replaceStr, spark_data.schema.fieldNames()))
                    spark_data = spark_data.toDF(*new_names)
                    # add year, month and day columns from timestamp for partition
                    #if proc.__name__ == 'query_generation':
                    #    spark_data = spark_data.withColumn("time", to_date("time_")).drop("'time_''")
                    spark_data = (spark_data
                                  .withColumn("year", year("time"))
                                  .withColumn("month", month("time"))
                                  .withColumn("day", dayofmonth("time"))
                                 )
                    
                    spark_data.write.mode('append').partitionBy("year").parquet('entsoe/'+proc.__name__)
                    print('')
                except NoMatchingDataError as e:
                    print('no data found for ',proc.__name__)
                except Exception as e:
                    print(e)

if __name__ == "__main__":  
    entsoe=False
    entsog=False
    if entsoe:
        client = EntsoePandasClient(api_key='ae2ed060-c25c-4eea-8ae4-007712f95375')
        print()
        country_code='DE'
        start = pd.Timestamp('20170131', tz='Europe/Berlin')
        delta=datetime.timedelta(days=90)
        end = start+delta
        
        procs= [client.query_day_ahead_prices,
                client.query_load,
                client.query_load_forecast,
                client.query_generation_forecast,
                client.query_wind_and_solar_forecast,
                client.query_generation,
                client.query_installed_generation_capacity ]
        
        entsoeToParquet(['DE'],procs,start,delta,3*4)
    
        #dat= pullData(client.query_wind_and_solar_forecast,country_code,start,start+datetime.timedelta(days=4))
    
    if entsog:
        # ENTSOG
        from entsog_api import yieldData,getDataFrame
        
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
        for name in names:
            print(name)
            t = time.time()
            data = getDataFrame(name)
            data.to_parquet('entsog/'+name+'.parquet')
            #spark_data = spark.read.parquet(name+'.parquet')
            print(time.time()-t)
            
        print(time.time()-t_ges)
    
        name = 'operationaldata'
        print('getting values from',name)
        for end1, phys in yieldData(name,bulks=365*3):
            print(end1)
            phys.to_parquet('entsog/'+name+'.parquet')
            spark_data = spark.read.parquet('entsog/'+name+'.parquet')            
            
            spark_data = (spark_data.withColumn("year", year("periodFrom"))
                    .withColumn("month", month("periodFrom"))
                    .withColumn("day", dayofmonth("periodFrom"))
                    .withColumn("time", to_timestamp("periodFrom"))
                    .withColumn("hour", hour("periodFrom"))
                    )
            spark_data.write.mode('append').partitionBy("year","month").parquet('entsog/'+name)
    
        name = 'AggregatedData'
        print('getting values from',name)
        for end1, phys in yieldData(name,bulks=52*3):
            print(end1)
            spark_data = spark.createDataFrame(phys)
            #phys.to_sql(name,conn, if_exists='append')
            spark_data.write.mode('append').partitionBy("year","month").parquet('entsog/'+name)