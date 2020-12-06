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
        selectString += 'toCountryKey, toOperatorKey,toOperatorLabel, toPointKey, toPointLabel, toBzKey,toBzLabel'
        
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
    
    def physicalFlow(self, operatorKeys: List[str], filt: Filter,group_by='directionKey'):
        whereString='"{}" < periodFrom and periodFrom < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        inString = '("'+'","'.join(operatorKeys)+'")'
        whereString+=f'and operatorKey in {inString}'
        selectString = f'strftime("{ftime[filt.groupby]}", "periodFrom") as time, '
        selectString+= 'pointKey, pointLabel, operatorKey, operatorLabel, directionKey, sum(value) as value, indicator'
        groupString=f'strftime("{ftime[filt.groupby]}", "time"), {group_by}'

        with closing(sql.connect(self.database)) as conn:
            query = f'select {selectString} from operationaldata where {whereString} group by {groupString}'
            flow = pd.read_sql_query(query, conn,index_col='time')
        return flow
    
    def physicalFlowByPoints(self, points: List[str], filt: Filter,group_by='directionKey'):
        whereString='"{}" < periodFrom and periodFrom < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        inString = '("'+'","'.join(points)+'")'
        whereString+=f'and pointKey in {inString}'
        selectString = f'strftime("{ftime[filt.groupby]}", "periodFrom") as time, '
        selectString+= 'pointKey, pointLabel, operatorKey, operatorLabel, directionKey, sum(value) as value, indicator'
        groupString=f'strftime("{ftime[filt.groupby]}", "time"), {group_by}'

        with closing(sql.connect(self.database)) as conn:
            query = f'select {selectString} from operationaldata where {whereString} group by {groupString}'
            flow = pd.read_sql_query(query, conn,index_col='time')
        return flow
    
    
    def bilanz(self, bz: str, filt: Filter):
        with closing(sql.connect(self.database)) as conn:
            query = f'select  fromOperatorKey from Interconnections where "{bz}"=fromBzLabel'
            operatorKeys=pd.read_sql_query(query, conn).dropna()['fromOperatorKey'].unique()
        inString = '("'+'","'.join(operatorKeys)+'")'
        
        whereString='"{}" < periodFrom and periodFrom < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        whereString+=f'and operatorKey in {inString}'
        
        selectString = f'strftime("{ftime[filt.groupby]}", "periodFrom") as time, '
        selectString+= 'infrastructureKey, directionKey, sum(value) as value'
        groupString=f'strftime("{ftime[filt.groupby]}", "time")'

        with closing(sql.connect(self.database)) as conn:
            query = f'select {selectString} from operationaldata o left join connectionpoints c on o.pointKey=c.pointKey where {whereString} group by {groupString}, directionKey, c.infrastructureKey'
            print(query)
            bil = pd.read_sql_query(query, conn,index_col='time')
        return bil
        
    
    def diffHelper(self,df):
        l = []
        p = pd.DataFrame()
        for col in df.columns:
            if col[2] not in l:
                for col2 in df.columns:
                    if col[2]==str(col2[2]) and col!=col2:
                        # same category
                        l.append(str(col[2]))
                        if col[1]=='entry':
                            print(col,col2)
                            p[str(col[2])]=df[col]-df[col2]
                        else: # entry - exit
                            p[str(col[2])]=df[col2]-df[col]
                    
                if str(col[2]) not in l:
                    if col[1]=='entry':
                        p[str(col[2])]=df[col]
                    else:
                        p[str(col[2])]=-df[col]
        return p
                    
        
        
    
    
    
    def crossborderFlows(self, bz: str, filt: Filter,group_by='directionKey'):
        with closing(sql.connect(self.database)) as conn:
            query = f'select distinct fromOperatorKey from Interconnections where "{bz}"=fromBzLabel'
            operatorKeys=pd.read_sql_query(query, conn).dropna().unique()
        inString = '("'+'","'.join(operatorKeys)+'")'
        
        whereString=f'"{filt.begin.strftime("%Y-%m-%d")}" < time and time < "{filt.end.strftime("%Y-%m-%d")}"'
        whereString+=f'and operatorKey in {inString}'
        
        selectString = f'strftime("{ftime[filt.groupby]}", "periodFrom") as time, '
        selectString+= 'pointKey, pointLabel, operatorKey, operatorLabel, directionKey, sum(value) as value, indicator'
        groupString=f'strftime("{ftime[filt.groupby]}", "time"), {group_by}'
        # TODO
        with closing(sql.connect(self.database)) as conn:
            query = f'select {selectString} from operationaldata where {whereString} group by {groupString}'
            print(query)
            flow = pd.read_sql_query(query, conn,index_col='time')
        return flow
    
if __name__ == "__main__":  

    entsog = EntsogSQLite('entsog.db')
    operators = entsog.operators()
    
    start=datetime(2017,7,1)
    end=datetime(2017,7,22)
    group='hour'
    filt = Filter(start,end,group)
    balzones = entsog.balancingzones()
    intercon = entsog.interconnections()
    cpp = entsog.connectionpoints()
    #gen = generation.melt(var_name='kind', value_name='value',ignore_index=False)    
    operatorLabels = ['bayernets', 'sdf']
    
    phy = entsog.physicalFlow(operatorLabels,filt)
    
    bil = entsog.bilanz('GASPOOL', filt)
    bb_ncg = bil.pivot(columns=['directionKey','infrastructureKey'])
    
    bb_ncg.plot(rot=45)
    
    p = entsog.diffHelper(bb_ncg)

    p.plot(rot=45)
    
