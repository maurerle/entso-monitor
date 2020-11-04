# -*- coding: utf-8 -*-
"""
Created on Tue Nov  3 13:03:22 2020

@author: fmaurer
"""
from entsog_api import *
# point Data
transportData = getDataFrame('operationaldatas')
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
