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

    def operationaldata(self, operatorKeys: List[str], filt: Filter,group_by: List[str]=['directionKey'], table='operationaldata'):
        whereString='"{}" < periodFrom and periodFrom < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        inString = '("'+'","'.join(operatorKeys)+'")'
        whereString+=f'and t.operatorKey in {inString}'
        
        if table == 'operationaldata':
            joinString =' left join (select distinct pointKey, isDoubleReporting, operatorKey, pipeInPipeWithTsoKey from operatorpointdirections) opd on t.pointKey = opd.pointKey and t.operatorKey = opd.operatorKey'
            whereString+= ' and pipeInPipeWithTsoKey is null and isDoubleReporting is null'
        else:
            joinString = ''
                
        
        selectString = f'strftime("{ftime[filt.groupby]}", "periodFrom") as time, '
        selectString+= 't.operatorKey, t.operatorLabel, t.directionKey, sum(value) as value'
        group_by =', '.join(list(map(lambda x: 't.'+x,group_by)))
        groupString=f'strftime("{ftime[filt.groupby]}", "time"), {group_by}'
        

        with closing(sql.connect(self.database)) as conn:
            query = f'select {selectString} from {table} t {joinString} where {whereString} group by {groupString}'
            flow = pd.read_sql_query(query, conn,index_col='time')
        return flow
    
    def operationaldata2(self, pointKeys: List[str], filt: Filter,group_by: List[str]=['directionKey'], table='operationaldata'):
        whereString='"{}" < periodFrom and periodFrom < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        inString = '("'+'","'.join(operatorKeys)+'")'
        whereString+=f'and t.operatorKey in {inString}'
        
        if table == 'operationaldata':
            joinString =' left join (select distinct pointKey, isDoubleReporting, operatorKey, pipeInPipeWithTsoKey from operatorpointdirections) opd on t.pointKey = opd.pointKey and t.operatorKey = opd.operatorKey'
            whereString+= ' and pipeInPipeWithTsoKey is null and isDoubleReporting is null'
        else:
            joinString = ''
                
        
        selectString = f'strftime("{ftime[filt.groupby]}", "periodFrom") as time, '
        selectString+= 't.operatorKey, t.operatorLabel, t.directionKey, sum(value) as value'
        group_by =', '.join(list(map(lambda x: 't.'+x,group_by)))
        groupString=f'strftime("{ftime[filt.groupby]}", "time"), {group_by}'
        

        with closing(sql.connect(self.database)) as conn:
            query = f'select {selectString} from {table} t {joinString} where {whereString} group by {groupString}'
            flow = pd.read_sql_query(query, conn,index_col='time')
        return flow    
    
    def operationaldataByPoints(self, points: List[str], filt: Filter,group_by: List[str]=['directionKey'],table='operationaldata'):
        whereString='"{}" < periodFrom and periodFrom < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        inString = '("'+'","'.join(points)+'")'
        whereString+=f'and pointKey in {inString}'
        selectString = f'strftime("{ftime[filt.groupby]}", "periodFrom") as time, '
        selectString+= 'pointKey, pointLabel, operatorKey, operatorLabel, directionKey, sum(value) as value, indicator'
        
        group_by =', '.join(group_by)
        groupString=f'strftime("{ftime[filt.groupby]}", "time"), {group_by}'

        with closing(sql.connect(self.database)) as conn:
            query = f'select {selectString} from {table} where {whereString} group by {groupString}'
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
        selectString+= 'infrastructureLabel, directionKey, sum(value) as value'
        groupString=f'strftime("{ftime[filt.groupby]}", "time")'

        with closing(sql.connect(self.database)) as conn:
            query = f'select {selectString} from operationaldata o left join connectionpoints c on o.pointKey=c.pointKey where {whereString} group by {groupString}, directionKey, c.infrastructureKey'
            print(query)
            bil = pd.read_sql_query(query, conn,index_col='time')
        return bil
        
    
    def diffHelper(self,df):
        '''
        gets difference for each pair of entry and exit direction index column
        '''
        l = []
        p = pd.DataFrame()
        for col in df.columns:
            if col[0] not in l:
                for col2 in df.columns:
                    if col[0]==str(col2[0]) and col!=col2:
                        # same category
                        l.append(str(col[0]))
                        if col[1]=='entry':
                            p[str(col[0])]=df[col]-df[col2]
                        else: # entry - exit
                            p[str(col[0])]=df[col2]-df[col]
                    
                if str(col[0]) not in l:
                    if col[1]=='entry':
                        p[str(col[0])]=df[col]
                    else:
                        p[str(col[0])]=-df[col]
        return p
                    
        
    def crossborder(self, bz: str, filt: Filter,group_by: List[str]=['t.directionKey','opd.adjacentZones','opd.adjacentCountry'], table='operationaldata'):
        whereString='"{}" < periodFrom and periodFrom < "{}"'.format(filt.begin.strftime("%Y-%m-%d"),filt.end.strftime("%Y-%m-%d"))
        
        with closing(sql.connect(self.database)) as conn:
            query = f'select distinct fromOperatorKey from Interconnections where "{bz}"=fromBzLabel'
            operatorKeys=pd.read_sql_query(query, conn).dropna()['fromOperatorKey'].unique()
        inString = '("'+'","'.join(operatorKeys)+'")'
        whereString+=f'and t.operatorKey in {inString}'
        
        if table == 'operationaldata':
            joinString =' left join (select distinct pointKey, isDoubleReporting, operatorKey, pipeInPipeWithTsoKey, adjacentZones, adjacentCountry from operatorpointdirections) opd on t.pointKey = opd.pointKey and t.operatorKey = opd.operatorKey'
            whereString+= ' and pipeInPipeWithTsoKey is null and isDoubleReporting is null'
        else:
            joinString =' left join (select distinct pointKey, isDoubleReporting, operatorKey, pipeInPipeWithTsoKey, adjacentZones, adjacentCountry from operatorpointdirections) opd on t.pointKey = opd.pointKey and t.operatorKey = opd.operatorKey'
                
        
        selectString = f'strftime("{ftime[filt.groupby]}", "periodFrom") as time, '
        selectString+= 't.directionKey, adjacentCountry, adjacentZones, sum(value) as value'
        group_by =', '.join(list(map(lambda x: ''+x,group_by)))
        groupString=f'strftime("{ftime[filt.groupby]}", "time"), {group_by}'

        with closing(sql.connect(self.database)) as conn:
            query = f'select {selectString} from {table} t {joinString} where {whereString} group by {groupString}'
            flow = pd.read_sql_query(query, conn,index_col='time')
            
        flow['name']=flow['adjacentCountry'].apply(lambda x: str(x) if x else '-')+':'+flow['adjacentZones'].apply(lambda x: str(x) if x else '-')
        del flow['adjacentCountry']
        del flow['adjacentZones']
    
        pivoted = flow.pivot(columns=['name','directionKey'],values='value')
        return self.diffHelper(pivoted)
    
if __name__ == "__main__":  

    entsog = EntsogSQLite('entsog.db')
    operators = entsog.operators()
    
    start=datetime(2018,7,1)
    end=datetime(2018,7,22)
    group='hour'
    filt = Filter(start,end,group)
    balzones = entsog.balancingzones()
    intercon = entsog.interconnections()
    cpp = entsog.connectionpoints()
    #gen = generation.melt(var_name='kind', value_name='value',ignore_index=False)    
    operatorKeys = ['DE-TSO-0004','DE-TSO-0007','DE-TSO-0005','DE-TSO-0006']
    
    phy = entsog.operationaldata(operatorKeys,filt,group_by=['operatorKey', 'directionKey'])
    piv  = phy.pivot(columns=['operatorKey','directionKey'],values='value')
    
    point = entsog.operationaldataByPoints(['ITP-00043','ITP-00111'],Filter(start,end,group),['pointKey', 'directionKey'])
    point['point']=point['pointLabel']+' '+point['directionKey']+' '+point['indicator']
    point['value']=point['value']/1e6
    piv2 = point.pivot(columns=['point'],values='value')
    
    bil = entsog.bilanz('NCG', filt)
    bb_ncg = bil.pivot(columns=['infrastructureLabel','directionKey'])
    
    bb_ncg.plot(rot=45)
    bb_ncg.columns = bb_ncg.columns.droplevel(None)
    
    p = entsog.diffHelper(bb_ncg)
    p.plot(rot=45)
    
    filt = Filter(start,end,'day')
    c = entsog.crossborder('GASPOOL', filt)
    c.plot(rot=45)
    
    # 55 mrd Nm^3 sind 60 GWh
    # 55 000 000 000 = 60 000 000 000 Wh pro Jahr
    # taeglich circa 160 000 000 Wh also 160 MWh
    # tatsaechlich 1 131 141 436 kWh pro Tag?? laut entsog