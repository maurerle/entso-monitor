#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 09:28:17 2020

@author: maurer

Dieses Projekt dient der Datenaggregation der Transparenzstelle der
European Network of Transmission System Operators for Gas (ENTSO-G)
"""
import requests
import pandas as pd

api_endpoint = 'https://transparency.entsog.eu/api/v1/'

response = requests.get(api_endpoint+'operationaldatas')
data = pd.read_csv(api_endpoint+'AggregatedData.csv?limit=1000')
response = requests.get(api_endpoint+'AggregatedData?limit=1000')
data = pd.DataFrame(response.json()['AggregatedData'])


names=['operationaldatas',
 'cmpUnsuccessfulRequests',
 'cmpUnavailables',
 'cmpAuctions',
 'AggregatedData',
 'tariffssimulations',
 'tariffsfulls',
 'urgentmarketmessages',
 'connectionpoints',
 'operators',
 'balancingzones',
 'operatorpointdirections',
 'Interconnections',
 'aggregateInterconnections']

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
        except Exception as e:
            raise Exception("{} : {} ".format(e.reason, e.url))

    return data

import sqlite3 as sql
from contextlib import closing
import time
from datetime import datetime, timedelta



print(datetime.date(2020,11,5)-datetime.timedelta(days=4))

if __name__ == "__main__":
    with closing(sql.connect('entsog.db')) as conn:
        t_ges = time.time()
        for name in names:
            print(name)
            t = time.time()
            data = getDataFrame(name)
            print(time.time()-t)
            data.to_sql(name,conn, if_exists='replace')
        print(time.time()-t_ges)


    fr=f"{datetime.today()-timedelta(days=2):%Y-%m-%d}"
    to=f"{datetime.today():%Y-%m-%d}"
    data = getDataFrame('operationaldatas',['from='+fr,'to='+to,'limit=-1'])
