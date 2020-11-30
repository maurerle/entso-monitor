#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 23:15:16 2020

@author: maurer
"""
from datetime import datetime
from typing import List
import pandas as pd

from entsog_data_manager import EntsogDataManager, Filter

import sqlite3 as sql
from contextlib import closing


class EntsogSQLite(EntsogDataManager):
    def __init__(self, database: str):
        self.database=database
        
    def countries(self):
        '''returns all the valid countries'''
        pass
    
    def interconnections(self, balancingZones: List[str], operators: List[str]):
        '''
        interconnections which are in one of the balancingZones
        to be determined whats useful here (coming from, to or both)
        '''
        selectString = 'pointKey, pointLabel, direction,operatorKey, fromOperatorLabel, fromCountryKey, fromBzKey'
        
        with closing(sql.connect(self.database)) as conn:
            interconnections = pd.read_sql_query(f'select * from Interconnections', conn)
        return interconnections
        
    def balancingZones(self):
        """also known as bidding zones"""
        selectString = 'tpMapX as lat, tpMapY as long, bzLabel'
        
        with closing(sql.connect(self.database)) as conn:
            zones = pd.read_sql_query(f'select {selectString} from balancingzones', conn)
        return zones
    
    def operators(self, country: str='', operatorType: str=''):
        '''
        returns operators which have an interconnection in one of the balancingZones
        '''
        if operatorType != '' and country != '' :
            whereString=f'where operatorTypeLabel="{operatorType}" and operatorCountryKey="{country}"'
        elif country != '':
            whereString=f'where operatorCountryKey="{country}"'
        elif operatorType != '' :
            whereString=f'where operatorTypeLabel="{operatorType}"'
        else:
            whereString = ''
            
        selectString = 'operatorKey, operatorLabel, operatorCountryKey, operatorTypeLabel'
        
        with closing(sql.connect(self.database)) as conn:
            operators = pd.read_sql_query(f'select {selectString} from operators {whereString}', conn)
        return operators
    
    def physicalFlow(self, balancingZones: List[str], pointKey, filt: Filter):
        pass
    
    def crossborderFlows(self, country: str, filt: Filter):
        pass
    
if __name__ == "__main__":  

    par = EntsogSQLite('entsog.db')
    operators = par.operators('DE')
    
    filt = Filter(datetime(2017,7,1),datetime(2017,7,2),'hour')
    balzones = par.balancingZones()
    intercon = par.interconnections(None, [''])
    #gen = generation.melt(var_name='kind', value_name='value',ignore_index=False)    