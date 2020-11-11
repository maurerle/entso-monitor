# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 13:58:33 2020

@author: fmaurer
"""
from entsoe import EntsoePandasClient

from entsoe.mappings import PSRTYPE_MAPPINGS,NEIGHBOURS,Area
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

start = pd.Timestamp('20201001', tz='Europe/Berlin')
end = pd.Timestamp('20201101', tz='Europe/Berlin')
country_code = 'DE'

day_ahead_prices = client.query_day_ahead_prices(country_code, start=start,end=end) # no data for DE
load = client.query_load(country_code, start=start,end=end)
try:
    load_forecast = client.query_load_forecast(country_code, start=start,end=end) # todo what is this
except:
    print('query load_forecast failed')
generation_forecast = client.query_generation_forecast(country_code, start=start,end=end)

# methods that return Pandas DataFrames
try:
    wind_and_solar_forecast=client.query_wind_and_solar_forecast(country_code, start=start,end=end, psr_type=None)
except:
    print('query wind_and_solar_forecast failed')
generation = client.query_generation(country_code, start=start,end=end, psr_type=None)
installed_generation_capacity = client.query_installed_generation_capacity(country_code, start=start,end=end, psr_type=None)
crossborders = client.query_crossborder_flows('DE', 'DK', start=start,end=end)
imbalance_prices = client.query_imbalance_prices(country_code, start=start,end=end, psr_type=None)
unavailability_of_generation_units = client.query_unavailability_of_generation_units(country_code, start=start,end=end, docstatus=None)
# not working, missing error handling
#withdrawn_unavailability_of_generation_units = client.query_withdrawn_unavailability_of_generation_units('NL', start=start,end=end)


# data = [day_ahead_prices,
#         load,load_forecast,
#         generation_forecast,
#         wind_and_solar_forecast,
#         generation,
#         installed_generation_capacity,
#         crossborders,
#         imbalance_prices,
#         unavailability_of_generation_units]