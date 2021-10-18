#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 21:59:08 2020

@author: maurer
"""

from datetime import datetime
from typing import List

physFlowTableName = 'Physical Flow'


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
        """also known as bidding zones"""
        pass

    def operators(self):
        '''
        returns operators which have an interconnection in one of the balancingZones
        '''
        pass

    def operatorpointdirections(self):
        pass

    def operationaldata(self, operatorKeys: List[str], filt: Filter, group_by: List[str] = ['directionKey'], table=physFlowTableName):
        pass

    def operationaldataByPoints(self, points: List[str], filt: Filter, group_by: List[str] = ['directionKey'], table=physFlowTableName):
        pass

    def operatorsByBZ(self, bz: str):
        pass

    def bilanz(self, operatorKeys: List[str], filt: Filter, table=physFlowTableName):
        pass

    def crossborder(self, operatorKeys: List[str], filt: Filter, group_by: List[str] = ['t.directionKey', 'opd.adjacentZones', 'opd.adjacentCountry'], table=physFlowTableName):
        pass
