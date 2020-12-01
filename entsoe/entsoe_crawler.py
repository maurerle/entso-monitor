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

#areas = [e.name for e in Area]
countries = list(filter(lambda x: len(x)<=2,areas[0]))

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

def calcDiff(data, inplace=False):
    if not inplace:
        dat = data.copy()
    else:
        dat=data
    for c in filter(lambda x:x.endswith('_Actual_Aggregated'), dat.columns):        
        new = str.replace(c,'_Actual_Aggregated','')
        dif = list(filter(lambda x: x.endswith('_Actual_Consumption') and x.startswith(new), dat.columns ))
        if len(dif) > 0:
            # wenn es beides gibt wird die Differenz gebildet
            dat[new]=dat[c]-dat[dif[0]]
            del dat[c]
            del dat[dif[0]]
        else:
            # sonst wird direkt 
            dat[new]=dat[c]
            del dat[c]
    for c in filter(lambda x:x.endswith('_Actual_Consumption'), dat.columns):
        # wenn es nur Verbrauch aber kein Erzeugnis gibt, mach negativ
        new = str.replace(c,'_Actual_Consumption','')
        dat[new]=-dat[c]
        del dat[c]
    return dat
            

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
            # replace spaces and invalid chars in column names
            data.columns = list(map(replaceStr, map(str,data.columns)))
            data.fillna(0, inplace=True)
            # calculate difference betweeen agg and consumption
            data = calcDiff(data, inplace=True)
            # add country column
            data['country']=country
            if self.database != None:
                with closing(sql.connect(self.database)) as conn:
                    try:
                        data.to_sql(proc.__name__,conn,if_exists='append')
                    except Exception as e:
                        print(e)
                        # merge old data with new data
                        prev = pd.read_sql_query(f'select * from {proc.__name__}',conn, index_col='index')
                        dat = pd.concat([prev,data])
                        # convert type as pandas needs it
                        dat.index = dat.index.astype('datetime64[ns]')
                        dat.to_sql(proc.__name__,conn,if_exists = 'replace')
                        print(f'replaced table {proc.__name__}')

            if self.spark != None:
                data['time'] = data.index
                spark_data = self.spark.createDataFrame(data)
                
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
        
    def pullCrossboarders(self,start,delta,times,proc,allZones=True):
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
                    try:
                        data.to_sql(proc.__name__,conn,if_exists='append')
                    except Exception as e:
                        print(e)
                        prev = pd.from_sql(proc.__name__,conn)
                        pd.concat([prev,data]).to_sql(proc.__name__,conn,if_exists = 'replace')
                        
            
            if self.spark != None:
                data['time']=data.index
                spark_data = self.spark.createDataFrame(data)
                spark_data.write.mode('append').parquet(f'{self.folder}/{proc.__name__}')    
    def pullPowerSystemData(self):
        df = pd.read_csv('https://data.open-power-system-data.org/conventional_power_plants/latest/conventional_power_plants_EU.csv')
        df.dropna(axis=0,subset=['lon','lat','eic_code'],inplace=True)
        df = df[['eic_code','name','company','country','capacity','energy_source','lon','lat']]
        # delete those without location or eic_code
        
        if self.database != None:
            with closing(sql.connect(self.database)) as conn:
                df.to_sql('powersystemdata',conn,if_exists='replace')
            
        if self.spark != None:
            df.to_parquet(f'{self.folder}/powersystemdata')
            #spark_data = spark.createDataFrame(df)
            #spark_data.write.mode('append').parquet(f'{self.folder}/powersystemdata') 
        return df

if __name__ == "__main__":  
    # Create a spark session
    conf = SparkConf().setAppName('entsoe').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print('')
    print('ENTSOE')

    client = EntsoePandasClient(api_key='ae2ed060-c25c-4eea-8ae4-007712f95375')    

    #country_code='DE'
    start = pd.Timestamp('20150101', tz='Europe/Berlin')
    #start = pd.Timestamp('20171216', tz='Europe/Berlin')
    delta=timedelta(days=90)
    end = start+delta
    
    times=6*4 # bis 2020    
    
    entsoe_path='hdfs://149.201.206.53:9000/user/fmaurer/entsoe'
    
    crawler = EntsoeCrawler(folder='data/spark',spark=None,database='data/entsoe.db')
    procs= [client.query_day_ahead_prices,
        client.query_load,
        client.query_load_forecast,
        client.query_generation_forecast,
        client.query_wind_and_solar_forecast,
        client.query_generation]
    # client.query_generation_per_plant
    # must be handled differently
    countries = [e.name for e in Area]
    countries = countries[1:] # finished DE_50HZ
    
    crawler.bulkDownload(countries,procs,start,delta,times)
    procs = [client.query_installed_generation_capacity, client.query_installed_generation_capacity_per_unit]
    #crawler.bulkDownload(countries,procs,start,delta=timedelta(days=365*6),times=1)
    
    crawler.pullPowerSystemData()
    proc= client.query_crossborder_flows
    crawler.pullCrossboarders(start,delta,times,proc)
    #cross = spark.read.parquet('data/query_crossborder_flows')
    #cross.repartition(1).write.parquet('data/query_crossborder_flows2'