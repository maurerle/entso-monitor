#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 23:26:05 2020

@author: maurer
"""

import requests
import pandas as pd
import time
import datetime
import sqlite3 as sql
from contextlib import closing
from tqdm import tqdm

def pd2par(dbname,table,parquet=''):
    '''
    converts sqlite database table to parquet file
    '''
    if parquet =='':
        parquet=table+'.parquet'
    with closing(sql.connect(dbname)) as conn:
        df = pd.read_sql_query('select * from '+table, conn)
        df.to_parquet(parquet)

def par2pd(dbname,parquet,table=''):
    '''
    converts parquet file to sqlite database table
    '''
    if table =='':
        table=parquet
    with closing(sql.connect(dbname)) as conn:
        df = pd.read_parquet(parquet)
        df.to_sql(table, conn)
        
def prerror():
    '''
    prints last thrown error
    '''
    import traceback,sys
    traceback.print_exception(etype=sys.last_type,value=sys.last_value,tb=sys.last_traceback)
    
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

########### ENTSOG #########

api_endpoint = 'https://transparency.entsog.eu/api/v1/'

'''
response = requests.get(api_endpoint+'operationaldatas')
data = pd.read_csv(api_endpoint+'AggregatedData.csv?limit=1000')
response = requests.get(api_endpoint+'AggregatedData?limit=1000')
data = pd.DataFrame(response.json()['AggregatedData'])
'''

import urllib

def getDataFrame(name, params=['limit=10000'], json=False):
    params_str = ''
    if len(params) > 0:
        params_str = '?'
    for param in params:
        params_str = params_str+param+'&'

    # csv is faster than json
    if json:
        response = requests.get(api_endpoint+name+params_str)
        if response.status_code != 200:
            raise Exception("{} {} : {} ".format(
                response.status_code, response.reason, response.url))
        data = pd.DataFrame(response.json()[name])
    else:
        try:
            data = pd.read_csv(api_endpoint+name+'.csv'+params_str)
        except requests.exceptions.InvalidURL as e:
            raise e
        except (requests.exceptions.HTTPError, urllib.error.HTTPError) as e:
            print("{} : {} ".format(e.reason, e.url))
            try:
                data = pd.read_csv(api_endpoint+name+'.csv'+params_str)
            except (requests.exceptions.HTTPError, urllib.error.HTTPError) as e:
                raise Exception("{} : {} ".format(e.reason, e.url))
    return data

def yieldData(name,indicator='Physical%20Flow',bulks=365*3,begin=datetime.date(2017,7,10)):
    if name == 'AggregatedData':
        step=1
    else:
        step=1
    delta = datetime.timedelta(days=step)

    end=begin+delta

    for i in range(int(bulks)):
        beg1 =begin+i*delta
        end1 =end + i*delta
        #pointDirection=DE-TSO-0001ITP-00096entry
        params = ['limit=-1','indicator='+indicator,'from='+str(beg1),'to='+str(end1),'periodType=hour']
        yield (beg1,end1), getDataFrame(name, params)

######## ENTSOE ###########

from entsoe.mappings import PSRTYPE_MAPPINGS,NEIGHBOURS,Area
from entsoe.exceptions import NoMatchingDataError
from pyspark.sql.functions import year, month, dayofmonth, to_timestamp, hour, to_date, trunc
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('entsoe').setMaster('local')
spark = SparkSession.builder.config(conf=conf).getOrCreate()

from_n = []
to_n = []
for n1 in NEIGHBOURS:
    for n2 in NEIGHBOURS[n1]:
        from_n.append(n1)
        to_n.append(n2)
neighbours=pd.DataFrame({'from':from_n,'to':to_n})


psrtype =pd.DataFrame.from_dict(PSRTYPE_MAPPINGS,orient='index')
areas = pd.DataFrame([[e.name,e.value,e._tz,e._meaning] for e in Area])

def pullData(procedure,country_code, start,end):
    t = time.time()
    data = pd.DataFrame(procedure(country_code,start=start,end=end))
    data['time'] = data.index
    dur = time.time()-t
    print(procedure.__name__, dur)
    return data
                    
def entsoeToParquet(country_code,proc,start,end):    
    try:
        data = pullData(proc,country_code, start, end)    
        spark_data = spark.createDataFrame(data)
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

        spark_data.write.mode('append').parquet('entsoe/'+country_code+'/'+proc.__name__)
        print('')
    except NoMatchingDataError:
        print('no data found for ',proc.__name__,start,end)
    except Exception as e:
        print('Error:',e)
        
def persistEntsoe(countries,procs,start,delta,times):
    end = start+delta
    for country_code in countries:
        # hier könnte man parallelisieren
        for proc in procs:
            pbar = tqdm(range(times))
            for i in pbar:
                start1 = start + i *delta
                end1 = end + i*delta
                pbar.set_description(country_code+' '+str(start1)+' '+str(end1))
                entsoeToParquet(country_code,proc, start1, end1)     
                    
def pullCrossboarders(start,delta,times,proc):
    end = start+delta
    for i in range(times):
        data = pd.DataFrame()
        start = start + delta
        end = end + delta
        for n1 in NEIGHBOURS:
            print(n1)
            
            for n2 in NEIGHBOURS[n1]:
                try:
                    dataN = proc(n1, n2, start=start,end=end)
                    # TODO
                    data[n1+'.'+n2]=dataN
                except Exception as e:
                    print(e)
                    
                    
        spark_data = spark.createDataFrame(data)
        spark_data.write.mode('append').parquet('entsoe/query_crossborder_flows')                