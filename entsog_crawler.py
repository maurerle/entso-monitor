# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 13:03:22 2020

@author: fmaurer
"""
from entsog_api import *
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


# SPARK TEST
# write to parquet
import findspark

findspark.init()


import pyspark
from pyspark.sql import SparkSession
import pandas as pd

# Create a spark session
spark = SparkSession.builder.getOrCreate()

# Create pandas data frame and convert it to a spark data frame
pandas_df = pd.DataFrame({"Letters":["X", "Y", "Z"]})
spark_df = spark.createDataFrame(pandas_df)

# Add the spark data frame to the catalog
spark_df.createOrReplaceTempView('spark_df')

spark_df.show()
spark_df.write.parquet("people.parquet")


df = spark.read.parquet("agg.parquet")
df.show() 

spark.catalog.listTables()

df.write.mode('append').partitionBy("year","month").parquet('AggregatedData')


teset2 = pd.read_parquet('AggregatedData')

test =spark.read.parquet('AggregatedData')

filter2= test.filter("month=8")
filter2.select("value").show()

test.show()

spark.sql("CREATE TEMPORARY VIEW PERSON2 USING parquet OPTIONS (path \"/tmp/output/people2.parquet/gender=F\")")

#dd = test['value'].toPandas() does not work, other runs out of mem

test.createOrReplaceTempView("tbl")
parkSQL = spark.sql("select value from tbl where month = 7")
dd = parkSQL.toPandas()

# visualize Data
conn = sql.connect('entsog.db')
query = "SELECT periodFrom,value FROM AggregatedData WHERE directionKey='entry' and adjacentSystemsLabel='IUK' and operatorKey='BE-TSO-0001'"

query = '''select value-exit_value as diff,a.periodFrom,value, exit_value from (SELECT periodFrom,value FROM AggregatedData WHERE directionKey='entry' and adjacentSystemsLabel='IUK') a
join (select periodFrom,value as exit_value from AggregatedData where directionKey='exit' and adjacentSystemsLabel='IUK') b
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