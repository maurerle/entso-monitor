#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 18:53:14 2020

@author: maurer
"""
import pandas as pd
from datetime import timedelta

from entsoe import EntsoePandasClient
from entsoe.mappings import PSRTYPE_MAPPINGS,NEIGHBOURS,Area
from entsoe.exceptions import NoMatchingDataError


import findspark
findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession

import sqlite3 as sql
from contextlib import closing

from tqdm import tqdm

from_n = []
to_n = []

for n1 in NEIGHBOURS:
    for n2 in NEIGHBOURS[n1]:
        from_n.append(n1)
        to_n.append(n2)
neighbours=pd.DataFrame({'from':from_n,'to':to_n})

psrtype =pd.DataFrame.from_dict(PSRTYPE_MAPPINGS,orient='index')
areas = pd.DataFrame([[e.name,e.value,e._tz,e._meaning] for e in Area])

countries = ['DE','BE','NL','AT','FR','CH','CZ','PL','GB','IT','HU','ES','FI','GR','MT','BA','LV','SK']

def replaceStr(string):
    '''
    replaces illegal values from a spark series name
    '''
    
    st = str.replace(string,')','')
    st = str.replace(st,'(','')
    st = str.replace(st,',','')
    st = str.replace(st,"'",'')
    st = st.strip()
    st = str.replace(st,' ','_')
    return st

class EntsoeCrawler:
    def __init__(self, folder, spark=None,database=None):
        self.spark = spark
        self.database = database
        self.folder = folder
    def pullData(self, procedure,country_code, start,end):
        data = pd.DataFrame(procedure(country_code,start=start,end=end))
        return data
    
    def persist(self, country, proc, start, end):    
        try:
            data = self.pullData(proc, country, start, end)    
            if self.database != None:
                # replace spaces and invalid chars in column names
                new_names = list(map(replaceStr, map(str,data.columns)))
                data.columns = new_names
                with closing(sql.connect(self.database)) as conn:
                    data.to_sql(f'{country}_{proc.__name__}',conn,if_exists='append')

            if self.spark != None:
                data['time'] = data.index
                spark_data = spark.createDataFrame(data)
                
                #new_names = list(map(replaceStr, spark_data.schema.fieldNames()))
                #spark_data = spark_data.toDF(*new_names)
                spark_data.write.mode('append').parquet(f'{self.folder}/{country}/{proc.__name__}')
        except NoMatchingDataError:
            print('no data found for ',proc.__name__,start,end)
        except Exception as e:
            print('Error:',e)
            
    def bulkDownload(self,countries,procs,start,delta,times):
        end = start+delta
        for country in countries:
            # hier könnte man parallelisieren
            for proc in procs:
                print()
                print(country,proc.__name__)
                pbar = tqdm(range(times))
                for i in pbar:
                    start1 = start + i *delta
                    end1 = end + i*delta
                    
                    pbar.set_description(f"{country} {start1:%Y-%m-%d} to {end1:%Y-%m-%d}")
                    self.persist(country,proc, start1, end1)               
        
    def pullCrossboarders(self,start,delta,times,proc,allZones=False):
        # reverse so that new relations exist
        end = start+times*delta
        for i in range(times):
            data = pd.DataFrame()
            start1 = end-(i+1)*delta
            end1 = end -i*delta
            for n1 in NEIGHBOURS:
                print(n1)
                
                for n2 in NEIGHBOURS[n1]:
                    try:
                        if (len(n1)==2 and len(n2)==2) or allZones:
                            dataN = proc(n1, n2, start=start1,end=end1)
                            data[n1+'.'+n2]=dataN
                    except Exception as e:
                        print(e)
                        
            if self.database != None:
                with closing(sql.connect(self.database)) as conn:
                    data.to_sql(f'{proc.__name__}',conn,if_exists='append')
            
            if self.spark != None:
                data['time']=data.index
                spark_data = spark.createDataFrame(data)
                spark_data.write.mode('append').parquet(f'{self.folder}/query_crossborder_flows')    

if __name__ == "__main__":  
    # Create a spark session
    conf = SparkConf().setAppName('entsoe').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print('')
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
    #country_code='DE'
    start = pd.Timestamp('20150101', tz='Europe/Berlin')
    #start = pd.Timestamp('20171216', tz='Europe/Berlin')
    delta=timedelta(days=90)
    end = start+delta
    
    times=6*4 # bis 2020
    #times=2 #  test    
    
    entsoe_path='hdfs://149.201.206.53:9000/user/fmaurer/entsoe'
    
    crawler = EntsoeCrawler('data',spark=spark,database='data/entsoe.db')
    #crawler.bulkDownload(countries,procs,start,delta,times)
    
    proc= client.query_crossborder_flows
    crawler.pullCrossboarders(start,delta,times,proc)
    
    #cross = spark.read.parquet('data/query_crossborder_flows')
    #cross.repartition(1).write.parquet('data/query_crossborder_flows2')