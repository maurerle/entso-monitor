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
from pyspark.sql.functions import year, month, to_timestamp

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

def getDataFrame(name, params=['limit=10000']):
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
            data = pd.read_csv(api_endpoint+name+'.csv'+params_str)
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

def yieldData(name,indicator='Physical%20Flow',bulks=365*3,begin=date(2017,7,10)):
    delta = timedelta(days=1)
    end=begin+delta

    for i in range(int(bulks)):
        beg1 =begin+i*delta
        end1 =end + i*delta
        params = ['limit=-1','indicator='+indicator,'from='+str(beg1),'to='+str(end1),'periodType=hour']
        time.sleep(5)
        # impact is quite small in comparison to 50s query length
        # rate limiting Gateway Timeo-outs
        yield (beg1,end1), getDataFrame(name, params)

if __name__ == "__main__":  
    try:
        spark
        print('using existing spark object')
    except:
        print('creating new spark object')
        name='entsoe'
        # master is 149.201.206.53
        # 149.201.225.* is client vpn address
        conf = SparkConf().setAppName('entsoe').setMaster('spark://master:7078')
        conf =  SparkConf().setAll([('spark.executor.memory', '3g'),
                               ('spark.executor.cores', '8'),
                               ('spark.cores.max', '8'),
                               ('spark.driver.memory', '9g'),
                               ("spark.app.name", name),
                               ("spark.master", "spark://master:7078"),
                               ("spark.driver.host", "149.201.225.67"),
                               ("spark.driver.bindAddress", "149.201.225.67")])
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        
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
    # for span, phys in tqdm(yieldData(name,bulks=365*3+90,begin=date(2017,7,10))):
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
    begin = date(2017,7,1)
    # test
    begin = date(2018,12,25)
    bulks = 365*2
    for span, phys in tqdm(yieldData(name,indicator='Physical%20Flow',bulks=bulks,begin=begin)):
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
        