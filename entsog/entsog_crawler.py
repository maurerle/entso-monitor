#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 12:04:45 2020

@author: maurer
"""

import requests
import urllib

import time
from datetime import datetime, date, timedelta
import pandas as pd
from tqdm import tqdm

api_endpoint = 'https://transparency.entsog.eu/api/v1/'

'''
response = requests.get(api_endpoint+'operationaldatas')
data = pd.read_csv(api_endpoint+'AggregatedData.csv?limit=1000')
response = requests.get(api_endpoint+'AggregatedData?limit=1000')
data = pd.DataFrame(response.json()['AggregatedData'])
'''

class EntsogCrawler:       
        
    def getDataFrame(self,name, params=['limit=10000']):
        params_str = ''
        if len(params) > 0:
            params_str = '?'
        for param in params[:-1]:
            params_str = params_str+param+'&'
        params_str+=params[-1]
    
        i=0
        data = pd.DataFrame()
        while i<10:            
            try:
                i+=1
                data = pd.read_csv(api_endpoint+name+'.csv'+params_str, index_col=False)
                break
            except requests.exceptions.InvalidURL as e:
                raise e
            except (requests.exceptions.HTTPError, urllib.error.HTTPError) as e:
                print("Error: {} : {} ".format(e.reason, e.url))
                if e.reason=='Gateway Time-out':
                    print('waiting 30 seconds..')
                    time.sleep(30)
        if data.empty:
            raise Exception('could not get any data for params:', params_str)
        return data

    def yieldData(self,name,indicator='Physical%20Flow',bulks=365*3,begin=date(2017,7,10)):
        delta = timedelta(days=1)
        end=begin+delta
    
        for i in range(int(bulks)):
            beg1 =begin+i*delta
            end1 =end + i*delta
            params = ['limit=-1','indicator='+indicator,'from='+str(beg1),'to='+str(end1),'periodType=hour']
            time.sleep(5)
            # impact is quite small in comparison to 50s query length
            # rate limiting Gateway Timeo-outs
            yield (beg1,end1), self.getDataFrame(name, params)

if __name__ == "__main__":  
    
    import sqlite3 as sql
    from contextlib import closing
    
    import findspark

    findspark.init()
    
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import year, month, to_timestamp


    try:
        spark
        print('using existing spark object')
    except:
        print('creating new spark object')
        appname='entsog'
        
        if True:
            conf = SparkConf().setAppName(appname).setMaster('local')
        else:
            # master is 149.201.206.53
            # 149.201.225.* is client vpn address
            conf =  SparkConf().setAll([('spark.executor.memory', '3g'),
                               ('spark.executor.cores', '8'),
                               ('spark.cores.max', '8'),
                               ('spark.driver.memory', '9g'),
                               ("spark.app.name", appname),
                               ("spark.master", "spark://master:7078"),
                               ("spark.driver.host", "149.201.225.67"),
                               ("spark.driver.bindAddress", "149.201.225.67")])
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        
    craw = EntsogCrawler()
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
    
    database = 'data/entsog.db'
    sparkfolder='data/spark'
    import os
    if not os.path.exists(sparkfolder):
        os.makedirs(sparkfolder)
    
    if False:
        for name in pbar:
            try:
                pbar.set_description(name)
                t = time.time()
                data = craw.getDataFrame(name)
                data.to_parquet(f'{sparkfolder}/{name}.parquet')
                with closing(sql.connect(database)) as conn:
                    data.to_sql(name,conn,if_exists='append')
                #spark_data = spark.read.parquet(name+'.parquet')
                #print(time.time()-t)
            except Exception as e:
                print(e)
        
    print(time.time()-t_ges)
    
    # unneeded, as we have operationalData
    if False:
        name = 'AggregatedData'
        print('getting values from',name)
        tt = time.time()
        for span, phys in tqdm(craw.yieldData(name,bulks=365*3+90,begin=date(2017,7,10))):
            print(span)
            spark_data = spark.createDataFrame(phys)
            #phys.to_sql(name,conn, if_exists='append')
            spark_data.write.mode('append').partitionBy("year","month").parquet(f'{sparkfolder}/{name}')
            print('finished',span[0],span[1],time.time()-tt)
            tt=time.time()

    if True:
        name = 'operationaldata'
        print('getting values from',name)
        tt = time.time()
        bulks = 365*3+4*30
        begin = date(2017,7,1)
        
        # test
        begin = date(2020,5,18)
        bulks = 30*5
        
        pbar = tqdm(craw.yieldData(name,indicator='Physical%20Flow',bulks=bulks,begin=begin))
        for span, phys in pbar:
            
            pbar.set_description(f'op {span[0]} to {span[1]}')
            with closing(sql.connect(database)) as conn:
                    phys.to_sql(name,conn,if_exists='append')
            
            phys.to_parquet(f'{sparkfolder}/{"temp"+name}.parquet')
            spark_data = spark.read.parquet(f'{sparkfolder}/{"temp"+name}.parquet')            
            spark_data = (spark_data
                            .withColumn('year', year('periodFrom'))
                            .withColumn('month', month('periodFrom'))
                            # .withColumn("day", dayofmonth("periodFrom"))
                            .withColumn("time", to_timestamp("periodFrom"))
                            # .withColumn("hour", hour("periodFrom"))
                            )
            spark_data.write.mode('append').parquet(f'{sparkfolder}/{name}')
            #print('finished',span[0],span[1],time.time()-tt)
            tt=time.time()
            
        with closing(sql.connect(database)) as conn:
            query = (f'CREATE INDEX IF NOT EXISTS "idx_opdata" ON "operationaldata" ("operatorKey","periodFrom");')
            conn.execute(query)       
            # sqlite will only use one index. EXPLAIN QUERY PLAIN shows if index is used
            # ref: https://www.sqlite.org/optoverview.html#or_optimizations
            # reference https://stackoverflow.com/questions/31031561/sqlite-query-to-get-the-closest-datetime