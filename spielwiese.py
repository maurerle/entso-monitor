# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 13:58:33 2020

@author: fmaurer
"""

###################### ENTSO-E ##################
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
import sqlite3
from contextlib import closing
    
abc = pd.read_parquet('operationaldata.parquet')


###################### ENTSO-G ##################

from entsog_api import getDataFrame,yieldData
# point Data
transportData = getDataFrame('operationaldata',['limit=1000'])
cmpUnsuccessfulRequest= getDataFrame('cmpUnsuccessfulRequests')
cmpUnavailableFirmCapacities = getDataFrame('cmpUnavailables')
cmpAuctionPremiums = getDataFrame('cmpAuctions')
#interruptions = getDataFrame('interruptions')

# zone Data

'''Latest Nominations, Allocations, Physical Flow'''
transportData = getDataFrame('AggregatedData')

# tariffs

'''Simulation of all the costs for flowing 1
GWh/day/year for each IP per product type and
tariff period'''
tariffs_simulation = getDataFrame('tariffssimulations')

'''Information about the various tariff types and
components related to the tariffs'''
tariffsfull = getDataFrame('tariffsfulls')

# urgent market messages (UMM)
urgent_market_messages = getDataFrame('urgentmarketmessages')

# referential data
'''Interconnection Points'''
connection_points = getDataFrame('connectionpoints')

'''All operators connected to the transmission
system'''
operators = getDataFrame('operators')

'''European balancing zones'''
balancing_zones = getDataFrame('balancingzones')

'''All the possible flow directions, being combination
of an operator, a point, and a flow direction'''
operatorpoint_directions = getDataFrame('operatorpointdirections')

'''All the interconnections between an exit system
and an entry system'''
interconnections = getDataFrame('Interconnections')

'''All the connections between transmission system
operators and their respective balancing zones'''
aggregate_interconnections = getDataFrame('aggregateInterconnections')
# TODO, diese müssen von entry und exit auseinander sortiert werden



#################################


import sqlite3
conn = sqlite3.connect('entsog.db')
pandas_df = pd.read_sql('select * from operationaldata', conn)
pandas_df.to_parquet('operationaldata.parquet')

spark_df = spark.read.parquet("operationaldata.parquet")

spark_df.write.parquet("entsog/operationaldata.parquet")
# Add the spark data frame to the catalog
spark_df.createOrReplaceTempView('spark_df')

spark_df.show()
spark_df.write.parquet("operationaldata.parquet")

from pyspark.sql.functions import year, month, dayofmonth, to_timestamp, hour
opdata = (spark_df.withColumn("year", year("periodFrom"))
        .withColumn("month", month("periodFrom"))
        .withColumn("day", dayofmonth("periodFrom"))
        .withColumn("time", to_timestamp("periodFrom"))
        .withColumn("hour", hour("periodFrom"))
        )

opdata.select(['year','month','day','periodFrom','time','hour']).show()

spark.catalog.listTables()

spark_df.write.mode('append').partitionBy("year","month").parquet('opdata')

agg =pd.read_sql('select * from AggregatedData', conn)

filtered= agg.filter("month=8")
filtered.select("value").show()
filtered.show()

#dd = test['value'].toPandas() does not work, other runs out of mem

agg.createOrReplaceTempView("agg")
parkSQL = spark.sql("select value from agg where month = 7")
dd = parkSQL.toPandas()

"""
load = pd.DataFrame(client.query_load(country_code, start=start,end=end))
load['time'] = load.index
spark_load = spark.createDataFrame(load)
spark_load.rdd.getNumPartitions()
"""

spark_data = spark_load.withColumn("Year", year("time")).withColumn(
"Month", month("time")).withColumn("Day", dayofmonth("time"))

spark_data.schema
spark_data.write.mode('append').partitionBy("Year","Month").parquet("Load")

# visualize Data
conn = sqlite3.connect('entsog.db')
query = "SELECT periodFrom,value FROM AggregatedData WHERE directionkey='entry' and adjacentSystemsLabel='IUK' and operatorKey='BE-TSO-0001'"

query = '''select value-exit_value as diff,a.periodFrom,value, exit_value from (SELECT periodFrom,value FROM AggregatedData WHERE directionkey='entry' and adjacentSystemsLabel='IUK') a
join (select periodFrom,value as exit_value from AggregatedData where directionkey='exit' and adjacentSystemsLabel='IUK') b
on a.periodFrom = b.periodFrom
'''

#and operatorKey='BE-TSO-0001'

df = pd.read_sql_query(query,conn)
df['begin']=pd.to_datetime(df['periodFrom']).dt.to_period('M').dt.to_timestamp()
df['begin']=pd.to_datetime(df['periodFrom']).dt.floor('d')

sums = df.groupby('begin').sum()
sums['begin']=sums.index
sums.plot('begin','diff',rot=45)
sums.plot('begin',['value','exit_value'],rot=45)

df.plot("periodFrom","value", rot=45)