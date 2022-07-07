#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 21:59:08 2020

@author: maurer
"""

from datetime import datetime
from typing import List

physFlowTableName = 'physical_flow'

class Filter:
    def __init__(self, begin: datetime, end: datetime, groupby='day'):
        self.begin = begin
        self.end = end
        self.groupby = groupby


class EntsogDataManager:
    def connectionpoints(self):
        pass

    def interconnections(self):
        '''
        interconnections which are in one of the balancingZones
        to be determined whats useful hiere (coming from, to or both)
        '''
        pass

    def balancingzones(self):
        """
        returns a list of balancing zones.
        also known as bidding zones
        """
        pass

    def operators(self):
        '''
        returns a list of operators
        '''
        pass

    def operatorpointdirections(self):
        '''
        returns the result of the operatorpointdirections request
        '''
        pass

    def operationaldata(self, operatorKeys: List[str], filt: Filter, group_by: List[str] = ['directionkey'], table=physFlowTableName):
        '''
        returns timeseries data of the given Indicator
        '''
        pass

    def operationaldataByPoints(self, points: List[str], filt: Filter, group_by: List[str] = ['directionkey'], table=physFlowTableName):
        pass

    def operatorsByBZ(self, bz: str):
        pass

    def bilanz(self, operatorKeys: List[str], filt: Filter, table=physFlowTableName):
        pass

    def crossborder(self, operatorKeys: List[str], filt: Filter, group_by: List[str] = ['t.directionkey', 'opd.adjacentzones', 'opd.adjacentcountry'], table=physFlowTableName):
        pass
