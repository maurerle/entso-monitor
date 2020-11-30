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

ftime={'day': '%Y-%m-%d',
       'month': '%Y-%m-01',
       'year': '%Y-01-01',
       'hour': '%Y-%m-%d %H:00:00',
       'minute': '%Y-%m-%d %H:%M:00'}

class EntsogSQLite(EntsogDataManager):
    def __init__(self, database: str):
        self.database=database
        
    def connectionpoints(self):
        selectString = 'tpMapX as lat, tpMapY as long, pointKey, pointLabel'
        
        with closing(sql.connect(self.database)) as conn:
            zones = pd.read_sql_query(f'select {selectString} from connectionpoints', conn)
        return zones

    def interconnections(self):
        '''
        interconnections which are in one of the balancingZones
        to be determined whats useful here (coming from, to or both)
        '''
        selectString = 'pointTpMapY as lat, pointTpMapX as lon, fromDirectionKey, '
        selectString += 'pointKey, pointLabel, fromOperatorKey, fromOperatorLabel, fromCountryKey, fromBzKey, fromBzLabel, '
        selectString += 'toCountryKey, toOperatorLabel, toPointKey, toPointLabel, toBzKey,toBzLabel'
        
        with closing(sql.connect(self.database)) as conn:
            interconnections = pd.read_sql_query(f'select {selectString} from Interconnections', conn)
        return interconnections
        
    def balancingzones(self):
        """also known as bidding zones"""
        selectString = 'tpMapY as lat, tpMapX as lon, bzLabel'
        
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
    
    def operatorpointdirections(self):
        selectString = 'pointKey, pointLabel, operatorLabel, directionKey, tpTsoItemLabel, tpTsoBalancingZone, tpTsoCountry, '
        selectString += 'adjacentCountry, connectedOperators, adjacentOperatorKey, adjacentZones'
        
        with closing(sql.connect(self.database)) as conn:
            opd = pd.read_sql_query(f'select {selectString} from operatorpointdirections', conn)
        return opd
    
    def physicalFlow(self, balancingZones: List[str], pointKey, filt: Filter):
        timeString='"{}" < time and time < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        selectString = f'strftime("{ftime[filt.groupby]}", "periodFrom") as time, '
        selectString+= 'pointKey, pointLabel, operatorKey, operatorLabel, directionKey, value, indicator'
        groupString=f'strftime("{ftime[filt.groupby]}", "time")'
        # TODO
        with closing(sql.connect(self.database)) as conn:
            flow = pd.read_sql_query(f'select {selectString} from operationaldata where {timeString} group by {groupString}', conn)
        return flow
    
    def crossborderFlows(self, country: str, filt: Filter):
        pass
    
if __name__ == "__main__":  

    par = EntsogSQLite('entsog.db')
    operators = par.operators()
    
    filt = Filter(datetime(2017,7,1),datetime(2017,7,2),'hour')
    balzones = par.balancingzones()
    intercon = par.interconnections()
    cpp = par.connectionpoints()
    #gen = generation.melt(var_name='kind', value_name='value',ignore_index=False)    