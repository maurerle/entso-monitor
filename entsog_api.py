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
import time
import datetime


api_endpoint = 'https://transparency.entsog.eu/api/v1/'

response = requests.get(api_endpoint+'operationaldatas')
data = pd.read_csv(api_endpoint+'AggregatedData.csv?limit=1000')
response = requests.get(api_endpoint+'AggregatedData?limit=1000')
data = pd.DataFrame(response.json()['AggregatedData'])

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
        except requests.exceptions.HTTPError as e:
            print("{} : {} ".format(e.reason, e.url))
            try:
                data = pd.read_csv(api_endpoint+name+'.csv'+params_str)
            except (requests.exceptions.HTTPError, urllib.error.HTTPError) as e:
                raise Exception("{} : {} ".format(e.reason, e.url))

    return data

#@deprecated
def loadDataSeries(name,indicator='Physical%20Flow',step=7,bulks=52*3):
    delta = datetime.timedelta(days=step)
    begin=datetime.date(2020,11,10)-delta
    end=datetime.date(2020,11,10)

    physicalFlow = pd.DataFrame()
    for i in range(int(bulks)*7):
        beg1 =begin-i*delta
        end1 =end - i*delta + datetime.timedelta(days=step)
        print('from '+str(beg1)+' to '+str(end1))
        #pointDirection=DE-TSO-0001ITP-00096entry
        params = ['limit=-1','indicator='+indicator,'from='+str(beg1),'to='+str(end1),'periodType=hour']
        phys = getDataFrame(name, params)
        physicalFlow =pd.concat([physicalFlow,phys])
    return physicalFlow


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


#print(datetime.date(2020,11,5)-datetime.timedelta(days=4))

if __name__ == "__main__":
    import sqlite3 as sql
    from contextlib import closing
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


    with closing(sql.connect('entsog.db')) as conn:
        t_ges = time.time()
        for name in names:
            print(name)
            t = time.time()
            data = getDataFrame(name)
            print(time.time()-t)
            data.to_sql(name,conn, if_exists='replace')
        print(time.time()-t_ges)

        name = 'operationaldata'
        print('getting values from',name)
        for end1, phys in yieldData(name,bulks=365*1):
            print(end1)
            phys.to_sql(name,conn, if_exists='append')

        name = 'AggregatedData'
        print('getting values from',name)
        for end1, phys in yieldData(name,bulks=52*1):
            print(end1)
            phys.to_sql(name,conn, if_exists='append')


