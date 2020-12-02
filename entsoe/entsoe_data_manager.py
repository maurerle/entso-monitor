#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 28 09:30:46 2020

@author: maurer
"""
from datetime import datetime

class Filter:
    def __init__(self, begin: datetime, end: datetime, groupby='day'):
        self.begin = begin
        self.end = end
        self.groupby = groupby

class EntsoeDataManager:
    '''
    Assumptions on the DataManager:
    
    index is time series, everything else are values
    no date and month columns
    '''
    def capacity(self, country: str):
        pass
    
    def load(self, country: str, filt: Filter):
        pass
    
    def generation(self, country: str, filt: Filter):
        pass
    
    def genPerPlant(self, plant: str, filt: Filter):
        pass
    
    def crossborderFlows(self, country: str, filt: Filter):
        pass
    
    def countries(self):
        pass
    
    def climateImpact(self):
        pass
    

def revReplaceStr(string):
    '''
    replaces series names for better visualization
    '''
    st = str.replace(string,'sum(','')
    st = str.replace(st,')','')
    st = str.replace(st,'(','')
    st = str.replace(st,'_',' ')
    st = st.strip()
    
    return st