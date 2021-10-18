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

import sqlite3 as sql
from contextlib import closing

api_endpoint = 'https://transparency.entsog.eu/api/v1/'

fr = date(2020, 5, 18)
to = date.today()
data = pd.read_csv(
    api_endpoint+f'operationaldata.csv?limit=1000&indicator=Allocation&from={fr}&to={to}')

'''
response = requests.get(api_endpoint+'operationaldatas')
data = pd.read_csv(api_endpoint+'AggregatedData.csv?limit=1000')
response = requests.get(api_endpoint+'AggregatedData?limit=1000')
data = pd.DataFrame(response.json()['AggregatedData'])
'''


class EntsogCrawler:

    def __init__(self, database=None, sparkfolder=None):
        self.database=database
        self.sparkfolder = sparkfolder

    def getDataFrame(self, name, params=['limit=10000'], useJson=False):
        params_str = ''
        if len(params) > 0:
            params_str = '?'
        for param in params[:-1]:
            params_str = params_str+param+'&'
        params_str += params[-1]

        i = 0
        data = pd.DataFrame()
        while i < 10:
            try:
                i += 1
                if useJson:
                    response = requests.get(
                        api_endpoint+name+'.json'+params_str)
                    data = pd.DataFrame(response.json()[name])
                    # replace empty string with None
                    data.replace([''], [None], inplace=True)
                else:
                    data = pd.read_csv(api_endpoint+name+'.csv' +
                                       params_str, index_col=False)
                break
            except requests.exceptions.InvalidURL as e:
                raise e
            except (requests.exceptions.HTTPError, urllib.error.HTTPError) as e:
                print("Error: {} : {} ".format(e.reason, e.url))
                if e.reason == 'Gateway Time-out':
                    print('waiting 30 seconds..')
                    time.sleep(30)
        if data.empty:
            raise Exception('could not get any data for params:', params_str)
        return data

    def yieldData(self, name, indicator='Physical Flow', bulks=365*3, begin=date(2017, 7, 10)):
        delta = timedelta(days=1)
        end = begin+delta

        for i in range(int(bulks)):
            beg1 = begin+i*delta
            end1 = end + i*delta
            params = ['limit=-1', 'indicator='+urllib.parse.quote(indicator), 'from=' +
                      str(beg1), 'to='+str(end1), 'periodType=hour']
            time.sleep(5)
            # impact is quite small in comparison to 50s query length
            # rate limiting Gateway Timeo-outs
            yield (beg1, end1), self.getDataFrame(name, params)


    def pullData(self,names):
        pbar = tqdm(names)
        for name in pbar:
            try:
                pbar.set_description(name)
                # use Json as connectionpoints have weird csv
                # TODO Json somehow has different data
                # connectionpoints count differ
                # and tpTSO column are named tSO in connpointdirections
                data = self.getDataFrame(name, useJson=True)
                if self.sparkfolder:
                    data.to_parquet(f'{self.sparkfolder}/{name}.parquet')
                #spark_data = spark.read.parquet(name+'.parquet')

                if self.database:
                    with closing(sql.connect(self.database)) as conn:
                        data.to_sql(name, conn, if_exists='replace')

            except Exception as e:
                print(e)

        if 'operatorpointdirections' in names and self.database:
            with closing(sql.connect(self.database)) as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_opd" ON "operatorpointdirections" ("operatorKey", "pointKey","directionKey");')
                conn.execute(query)

    def pullOperationalData(self, indicators, begin=None, end=None):
        print('getting values from operationaldata')
        if not end:
            end = date.today()

        if not begin:
            try:
                with closing(sql.connect(self.database)) as conn:
                    query = f'select max("periodFrom") from "{indicators[0]}"'
                    d = conn.execute(query).fetchone()[0]
                begin = pd.to_datetime(d).date()
            except Exception as e:
                print(repr(e), 'using default start')
                begin = date(2017, 7, 10)

        bulks = (end-begin).days
        print(f'start: {begin}, end: {end}, days: {bulks}, indicators: {indicators}')

        if bulks < 1:
            return

        indicators = ['Physical Flow', 'Allocation', 'Firm Technical']
        #indicator = 'Allocation'

        for indicator in indicators:
            #database = indicator+'.db'
            pbar = tqdm(self.yieldData('operationaldata', indicator, bulks, begin))
            for span, phys in pbar:
                pbar.set_description(f'op {span[0]} to {span[1]}')

                if self.database:
                    with closing(sql.connect(self.database)) as conn:
                        phys.to_sql(indicator, conn, if_exists='append')

                if self.sparkfolder:
                    phys.to_parquet(f'{self.sparkfolder}/temp{indicator}.parquet')
                    spark_data = spark.read.parquet(
                        f'{self.sparkfolder}/temp{indicator}.parquet')
                    spark_data = (spark_data
                                  .withColumn('year', year('periodFrom'))
                                  .withColumn('month', month('periodFrom'))
                                  # .withColumn("day", dayofmonth("periodFrom"))
                                  .withColumn("time", to_timestamp("periodFrom"))
                                  # .withColumn("hour", hour("periodFrom"))
                                  )
                    spark_data.write.mode('append').parquet(
                        f'{self.sparkfolder}/{indicator}')

        # sqlite will only use one index. EXPLAIN QUERY PLAIN shows if index is used
        # ref: https://www.sqlite.org/optoverview.html#or_optimizations
        # reference https://stackoverflow.com/questions/31031561/sqlite-query-to-get-the-closest-datetime
        if 'Allocation' in names and self.database:
            with closing(sql.connect(self.database)) as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_opdata" ON "Allocation" ("operatorKey","periodFrom");')
                conn.execute(query)

                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_pointKey" ON "Allocation" ("pointKey","periodFrom");')
                conn.execute(query)
        if 'Physical Flow' in names and self.database:
            with closing(sql.connect(self.database)) as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_phys_operator" ON "Physical Flow" ("operatorKey","periodFrom");')
                conn.execute(query)

                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_phys_point" ON "Physical Flow" ("pointKey","periodFrom");')
                conn.execute(query)

        if 'Firm Technical' in names and self.database:
            with closing(sql.connect(self.database)) as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_ft_opdata" ON "Firm Technical" ("operatorKey","periodFrom");')
                conn.execute(query)

                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_ft_pointKey" ON "Firm Technical" ("pointKey","periodFrom");')
                conn.execute(query)

if __name__ == "__main__":
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
        appname = 'entsog'

        if True:
            conf = SparkConf().setAppName(appname).setMaster('local')
        else:
            # master is 149.201.206.53
            # 149.201.225.* is client vpn address
            conf = SparkConf().setAll([('spark.executor.memory', '3g'),
                                       ('spark.executor.cores', '8'),
                                       ('spark.cores.max', '8'),
                                       ('spark.driver.memory', '9g'),
                                       ("spark.app.name", appname),
                                       ("spark.master", "spark://master:7078"),
                                       ("spark.driver.host", "149.201.225.67"),
                                       ("spark.driver.bindAddress", "149.201.225.67")])
        spark = SparkSession.builder.config(conf=conf).getOrCreate()

    database = 'data/entsog.db'
    sparkfolder = 'data/spark'
    import os
    if not os.path.exists(sparkfolder):
        os.makedirs(sparkfolder)
    craw = EntsogCrawler(database, sparkfolder=None)

    names = ['cmpUnsuccessfulRequests',
             # 'operationaldata',
             #'cmpUnavailables',
             #'cmpAuctions',
             # 'AggregatedData', # operationaldata aggregated for each zone
             #'tariffssimulations',
             #'tariffsfulls',
             #'urgentmarketmessages',
             'connectionpoints',
             'operators',
             'balancingzones',
             'operatorpointdirections',
             'Interconnections',
             'aggregateInterconnections']

    craw.pullData(names)

    indicators = ['Physical Flow', 'Allocation', 'Firm Technical']
    craw.pullOperationalData(indicators)