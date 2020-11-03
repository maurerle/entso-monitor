#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 09:28:17 2020

@author: maurer
"""
import requests
import pandas as pd

api_endpoint = 'https://transparency.entsog.eu/api/v1/'

response = requests.get(api_endpoint+'operationaldatas').json()
#print(response['meta'])
#df = pd.read_json (response.content)

data = pd.DataFrame(d)
response = requests.get(api_endpoint+'AggregatedData?limit=1000').json()
data = pd.DataFrame(response['AggregatedData'])

def getDataFrame(name,params):
    params_str = ''
    if len(params) > 0:
        params_str = '?'
    for param in params:
        params_str= params_str+param+'&'
    response = requests.get(api_endpoint+name+params_str).json()
    return pd.DataFrame(response[name]),response['meta']

data, meta = getDataFrame('AggregatedData', ['limit=1000'])