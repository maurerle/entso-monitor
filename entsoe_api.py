# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 13:58:33 2020

@author: fmaurer
"""
from entsoe import EntsoePandasClient

from entsoe.mappings import PSRTYPE_MAPPINGS,NEIGHBOURS,Are
import pandas as pd

#neighbours = pd.DataFrame.from_dict(NEIGHBOURS,orient='index')
from_n = []
to_n = []
for n1 in NEIGHBOURS:
    for n2 in NEIGHBOURS[n1]:
        from_n.append(n1)
        to_n.append(n2)
neighbours=pd.DataFrame({'from':from_n,'to':to_n})


psrtype =pd.DataFrame.from_dict(PSRTYPE_MAPPINGS,orient='index')
areas = pd.DataFrame([[e.name,e.value,e._tz,e._meaning] for e in Area])


client = EntsoePandasClient(api_key='ae2ed060-c25c-4eea-8ae4-007712f95375')

'20160201'
start = pd.Timestamp('20160201', tz='Europe/Berlin')
end = pd.Timestamp('20161101', tz='Europe/Berlin')


def getData(start,end,country_code):
    # methods that return Pandas Series
    data= {}
    try:
        data['day_ahead_prices']=client.query_day_ahead_prices(country_code, start=start,end=end)
    except:
        print('query day_ahead_prices failed')

    try:
        data['load']=client.query_load(country_code, start=start,end=end)
        data['load_forecast']=client.query_load_forecast(country_code, start=start,end=end) # todo what is this
        data['generation_forecast']=client.query_generation_forecast(country_code, start=start,end=end)
        data['wind_and_solar_forecast']=client.query_wind_and_solar_forecast(country_code, start=start,end=end, psr_type=None)
        data['generation']=client.query_generation(country_code, start=start,end=end, psr_type=None)
        data['installed_generation_capacity']=client.query_installed_generation_capacity(country_code, start=start,end=end, psr_type=None)
    except Exception as e:
        print(e.message)

    if country_code in ['DE','AT','LU']:
        neighbour_code= 'DE_AT_LU'
    else:
        neighbour_code=country_code

    for neighbour in NEIGHBOURS[neighbour_code]:
        try:
            data['crossborders_'+neighbour]=client.query_crossborder_flows(country_code, neighbour, start=start,end=end)
        except:
            print('crossborder failed for: '+neighbour+' - '+neighbour_code)
    try:
        data['imbalance_prices']=client.query_imbalance_prices(country_code, start=start,end=end, psr_type=None)
        data['unavailability_of_generation_units']=client.query_unavailability_of_generation_units(country_code, start=start,end=end, docstatus=None)
    except Exception as e:
        print('query imbalance_prices failed')
        print(e.message)
    return data

import sqlite3 as sql
from contextlib import closing
import time
import datetime

if __name__ == "__main__":
    with closing(sql.connect('entsoe.db')) as conn:
        psrtype.to_sql('psrtype',conn, if_exists='replace')
        areas.to_sql('areas',conn, if_exists='replace')
        neighbours.to_sql('neighbours',conn, if_exists='replace')
        t_ges = time.time()
        country = 'DE'
        data= getData(start,end,country)
        for data_name in data:
            print(data_name)
            data[data_name].to_sql(data_name,conn, if_exists='replace')
        print(time.time()-t_ges)