#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Defines abstract Datamanagers for ENTSO-E
Created on Sat Nov 28 09:30:46 2020

@author: maurer
"""
from datetime import datetime
from typing import List

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
    
    def crossborderFlows(self, country: str, filt: Filter):
        pass
    
    def countries(self):
        pass
    
    def climateImpact(self):
        pass
    
class EntsoePlantDataManager:
    '''
    Assumptions on the PlantDataManager:
    
    index is time series, everything else are values
    '''
    def plantGen(self, names: List[str], filt: Filter):
        '''
        generation data per power plant 
        '''
        pass

    def getNames(self):
        '''
        returns a dataframe with all power plant names
        '''
        pass
    
    def capacityPerPlant(self, country=''):
        '''
        returns capacities for all plants of a country
        or all if country is empty
        '''
        pass

    def powersystems(self, country=''):
        '''
        returns location data and capacities of power plants
        '''
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