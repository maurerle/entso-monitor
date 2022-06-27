#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 12:04:45 2020

@author: maurer
"""

import requests
import urllib

import time
from datetime import date, timedelta
import pandas as pd
from tqdm import tqdm

import sqlite3
from contextlib import closing

from sqlalchemy import create_engine
from contextlib import contextmanager

import logging

logging.basicConfig()
log = logging.getLogger('entsog')
log.setLevel(logging.INFO)

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

    def __init__(self, database):
        if database.startswith('postgresql'):
            self.engine = create_engine(database)
            @contextmanager
            def access_db():
                with self.engine.connect() as conn, conn.begin():
                    yield conn

            self.db_accessor = access_db
        else:
            self.db_accessor = lambda: closing(sqlite3.connect(database))

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
                    data = data.replace([''], [None])
                else:
                    data = pd.read_csv(api_endpoint+name+'.csv' +
                                       params_str, index_col=False)
                break
            except requests.exceptions.InvalidURL as e:
                raise e
            except (requests.exceptions.HTTPError, urllib.error.HTTPError) as e:
                log.exception('Error getting Frame')
                if e.response.status_code >= 500 :
                    log.info(f'{e.response.reason} - waiting 30 seconds..')
                    time.sleep(30)
        if data.empty:
            raise Exception('could not get any data for params:', params_str)
        data.columns = [x.lower() for x in data.columns]
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
            # impact of sleeping here is quite small in comparison to 50s query length
            # rate limiting Gateway Timeouts
            df = self.getDataFrame(name, params)
            df['periodFrom'] = pd.to_datetime(df['periodFrom'], infer_datetime_format=True)
            yield (beg1, end1), df

    def pullData(self, names):
        pbar = tqdm(names)
        for name in pbar:
            try:
                pbar.set_description(name)
                # use Json as connectionpoints have weird csv
                # TODO Json somehow has different data
                # connectionpoints count differ
                # and tpTSO column are named tSO in connpointdirections
                data = self.getDataFrame(name, useJson=True)

                with self.db_accessor() as conn:
                    tbl_name = name.lower().replace(' ','_')
                    data.to_sql(tbl_name, conn, if_exists='replace')

            except Exception:
                log.exception('error pulling data')

        if 'operatorpointdirections' in names and self.db_accessor:
            with self.db_accessor() as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_opd" ON operatorpointdirections (operatorKey, pointKey,directionkey);')
                conn.execute(query)

    def pullOperationalData(self, indicators, begin=None, end=None):
        log.info('getting values from operationaldata')
        if not end:
            end = date.today()

        if not begin:
            try:
                if self.db_accessor:

                    with self.db_accessor() as conn:
                        tbl_name = indicators[0].lower().replace(' ','_')
                        query = f'select max(periodFrom) from {tbl_name}'
                        d = conn.execute(query).fetchone()[0]
                    begin = pd.to_datetime(d).date()
            except Exception:
                log.error('table does not exist yet - using default start')
                begin = date(2017, 7, 10)

        bulks = (end-begin).days
        log.info(
            f'start: {begin}, end: {end}, days: {bulks}, indicators: {indicators}')

        if bulks < 1:
            return

        #indicators = ['Physical Flow', 'Allocation', 'Firm Technical']
        #indicator = 'Allocation'

        for indicator in indicators:
            pbar = tqdm(self.yieldData(
                'operationaldata', indicator, bulks, begin))
            for span, phys in pbar:
                pbar.set_description(f'op {span[0]} to {span[1]}')

                tbl_name = indicator.lower().replace(' ','_')
                with self.db_accessor() as conn:
                    phys.to_sql(tbl_name, conn, if_exists='append')
                
                try:
                    with self.db_accessor() as conn:
                        query_create_hypertable = f"SELECT create_hypertable('{tbl_name}', 'periodfrom', if_not_exists => TRUE, migrate_data => TRUE);"
                        conn.execute(query_create_hypertable)
                        log.info(f'created hypertable {tbl_name}')
                except Exception as e:
                    log.error(f'could not create hypertable {tbl_name}: {e}')

        # sqlite will only use one index. EXPLAIN QUERY PLAIN shows if index is used
        # ref: https://www.sqlite.org/optoverview.html#or_optimizations
        # reference https://stackoverflow.com/questions/31031561/sqlite-query-to-get-the-closest-datetime
        if 'Allocation' in names and self.db_accessor:
            with self.db_accessor() as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_opdata" ON Allocation (operatorKey,periodFrom);')
                conn.execute(query)

                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_pointKey" ON Allocation (pointKey,periodFrom);')
                conn.execute(query)
        if 'Physical Flow' in names and self.db_accessor:
            with self.db_accessor() as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_phys_operator" ON Physical_Flow (operatorKey,periodFrom);')
                conn.execute(query)

                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_phys_point" ON Physical_Flow (pointKey,periodFrom);')
                conn.execute(query)

        if 'Firm Technical' in names and self.db_accessor:
            with self.db_accessor() as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_ft_opdata" ON Firm_Technical (operatorKey,periodFrom);')
                conn.execute(query)

                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_ft_pointKey" ON Firm_Technical (pointKey,periodFrom);')
                conn.execute(query)

if __name__ == "__main__":
    database = 'data/entsog.db'
    import os
    craw = EntsogCrawler(database)

    names = ['cmpUnsuccessfulRequests',
             # 'operationaldata',
             # 'cmpUnavailables',
             # 'cmpAuctions',
             # 'AggregatedData', # operationaldata aggregated for each zone
             # 'tariffssimulations',
             # 'tariffsfulls',
             # 'urgentmarketmessages',
             'connectionpoints',
             'operators',
             'balancingzones',
             'operatorpointdirections',
             'Interconnections',
             'aggregateInterconnections']

    craw.pullData(names)

    indicators = ['Physical Flow', 'Allocation', 'Firm Technical']
    craw.pullOperationalData(indicators)
