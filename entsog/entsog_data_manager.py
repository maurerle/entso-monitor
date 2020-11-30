#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 21:59:08 2020

@author: maurer
"""

from datetime import datetime
from typing import List

class Filter:
    def __init__(self, begin: datetime, end: datetime, groupby='day'):
        self.begin = begin
        self.end = end
        self.groupby = groupby

class EntsogDataManager:
    def countries(self):
        '''returns all the valid countries'''
        pass
    
    def interconnections(self, balancingZones: List[str], operators: List[str]):
        '''
        interconnections which are in one of the balancingZones
        to be determined whats useful hiere (coming from, to or both)
        '''
        pass
        
    def balancingZones(self):
        """also known as bidding zones"""
        pass
    
    def operators(self, balancingZones: List[str], operatorType: str):
        '''
        returns operators which have an interconnection in one of the balancingZones
        '''
        pass
    
    def physicalFlow(self, balancingZones: List[str], pointKey, filt: Filter):
        pass
    
    def generation(self, country: str, filt: Filter):
        pass
    
    def pumpStorage(self, country: str, filt: Filter):
        pass
    
    def crossborderFlows(self, country: str, filt: Filter):
        pass