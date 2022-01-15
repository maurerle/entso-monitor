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

import sqlite3
from contextlib import closing, contextmanager
from sqlalchemy import create_engine

ftime_sqlite = {'day': '%Y-%m-%d',
                'month': '%Y-%m-01',
                'year': '%Y-01-01',
                'hour': '%Y-%m-%d %H:00:00',
                'minute': '%Y-%m-%d %H:%M:00'}

ftime_pg = {'day': 'YYYY-MM-DD',
            'month': 'YYYY-MM-01',
            'year': 'YYYY-01-01',
            'hour': 'YYYY-MM-DD hh24:00:00',
            'minute': 'YYYY-MM-DD hh24:mi:00'}

checkPipeInPipe = "pipeinpipewithtsokey is NULL"
checkDoubleReporting = "isdoublereporting is not 1"


def timeFilter(filt):
    return f"'{filt.begin.strftime('%Y-%m-%d')}' < periodFrom and periodFrom < '{filt.end.strftime('%Y-%m-%d')}' "


physFlowTableName = 'Physical Flow'


class EntsogSQLite(EntsogDataManager):
    def __init__(self, database: str):
        if database:
            self.use_pg = database.startswith('postgresql')
            if self.use_pg:
                self.engine = create_engine(database)
                @contextmanager
                def access_db():
                    yield self.engine

                self.db_accessor = access_db
            else:
                self.db_accessor = lambda: closing(sqlite3.connect(database))
        else:
            self.db_accessor = None

    def groupTime(self, groupby, column):
        if self.use_pg:
            return f"to_char('{column}', '{ftime_pg[groupby]}')" # PostgreSQL
        else:
            return f'strftime("{ftime_sqlite[groupby]}", "{column}")' # SQLite
        

    def connectionpoints(self):
        selectString = 'tpMapX as lat, tpMapY as long, pointKey, pointlabel'

        with self.db_accessor() as conn:
            zones = pd.read_sql_query(
                f'select {selectString} from connectionpoints', conn)
        return zones

    def interconnections(self):
        '''
        interconnections which are in one of the balancingZones
        to be determined whats useful here (coming from, to or both)
        '''
        selectString = 'pointTpMapY as lat, pointTpMapX as lon, fromdirectionkey, '
        selectString += 'pointKey, pointlabel, fromOperatorKey, fromOperatorLabel, fromCountryKey, fromBzKey, frombzlabel, '
        selectString += 'toCountryKey, toOperatorKey,toOperatorLabel, toPointKey, topointlabel, toBzKey,toBzLabel'

        with self.db_accessor() as conn:
            interconnections = pd.read_sql_query(
                f'select {selectString} from Interconnections', conn)
        return interconnections

    def balancingzones(self):
        """also known as bidding zones"""
        selectString = 'tpMapY as lat, tpMapX as lon, bzLabel'

        with self.db_accessor() as conn:
            zones = pd.read_sql_query(
                f'select {selectString} from balancingzones', conn)
        return zones

    def operators(self, country: str = '', operatorType: str = ''):
        '''
        returns operators which have an interconnection in one of the balancingZones
        '''
        if operatorType != '' and country != '':
            whereString = f"where operatorTypeLabel='{operatorType}' and operatorCountryKey='{country}'"
        elif country != '':
            whereString = f"where operatorCountryKey='{country}'"
        elif operatorType != '':
            whereString = f'where operatorTypeLabel="{operatorType}"'
        else:
            whereString = ''

        selectString = 'operatorKey, operatorLabel, operatorCountryKey, operatorTypeLabel'

        with self.db_accessor() as conn:
            operators = pd.read_sql_query(
                f'select {selectString} from operators {whereString}', conn)
        return operators

    def operatorpointdirections(self):
        selectString = 'pointKey, pointlabel, operatorLabel, directionkey, '
        selectString += 'tpTsoItemLabel, tSOBalancingZone, tSOCountry, pipeinpipewithtsokey, isdoublereporting,'
        selectString += 'adjacentcountry, connectedOperators, adjacentOperatorKey, adjacentzones'

        with self.db_accessor() as conn:
            opd = pd.read_sql_query(
                f'select {selectString} from operatorpointdirections', conn)
        return opd

    def operationaldata(self, operatorKeys: List[str], filt: Filter, group_by: List[str] = ['directionkey'], table=physFlowTableName):
        whereString = timeFilter(filt)
        inString = '("'+'","'.join(operatorKeys)+'")'
        whereString += f'and t.operatorKey in {inString} and {checkDoubleReporting}'
        joinString = ' left join (select distinct pointKey, isdoublereporting, operatorKey, pipeinpipewithtsokey from operatorpointdirections) opd on t.pointKey = opd.pointKey and t.operatorKey = opd.operatorKey'

        if table == physFlowTableName:
            whereString += f' and {checkPipeInPipe}'

        selectString = f'{self.groupTime(filt.groupby, "periodfrom")} as time, '
        selectString += 't.operatorKey, t.operatorLabel, t.directionkey, sum(value) as value'
        group_by = ', '.join(list(map(lambda x: 't.'+x, group_by)))
        groupString = f'{self.groupTime(filt.groupby, "time")}, {group_by}'

        with self.db_accessor() as conn:
            query = f'select {selectString} from "{table}" t {joinString} where {whereString} group by {groupString}'
            flow = pd.read_sql_query(query, conn, index_col='time')
        return flow

    def operationaldataByPoints(self, points: List[str], filt: Filter, group_by: List[str] = ['directionkey'], table=physFlowTableName):
        whereString = timeFilter(filt)
        inString = '("'+'","'.join(points)+'")'
        whereString += f'and pointKey in {inString}'
        selectString = f'{self.groupTime(filt.groupby, "periodfrom")} as time, '
        selectString += 'pointKey, pointlabel, operatorKey, operatorLabel, '
        selectString += 'directionkey, sum(value) as value, indicator, pipeinpipewithtsokey'
        joinString = ' left join (select distinct pointKey as pk, isdoublereporting, operatorKey as ok, pipeinpipewithtsokey from operatorpointdirections) opd on t.pointKey = opd.pk and t.operatorKey = opd.ok'

        group_by = ', '.join(group_by)
        groupString = f'{self.groupTime(filt.groupby, "time")}, {group_by}'

        with self.db_accessor() as conn:
            query = f'select {selectString} from "{table}" t {joinString} where {whereString} group by {groupString}'
            flow = pd.read_sql_query(query, conn, index_col='time')
        return flow

    def operatorsByBZ(self, bz: str):
        with self.db_accessor() as conn:
            query = f'select distinct fromOperatorKey from Interconnections where "{bz}"=frombzlabel'
            operatorKeys = pd.read_sql_query(query, conn).dropna()[
                'fromoperatorkey'].unique()
        return operatorKeys

    def bilanz(self, operatorKeys: List[str], filt: Filter, table=physFlowTableName):
        inString = '("'+'","'.join(operatorKeys)+'")'

        whereString = timeFilter(filt)
        whereString += f' and o.operatorKey in {inString} and {checkDoubleReporting}'

        selectString = f'{self.groupTime(filt.groupby, "periodfrom")} as time, '
        # TODO if connectionpoints would not have missing data, remove this hack
        # this is using that the pointKeys first 3 chars are generelly indicating
        # the infrastructureKey
        selectString += 'coalesce(c.infrastructureKey, substr(o.pointKey,0,4)) as infra, directionkey, sum(value) as value'
        groupString = f'{self.groupTime(filt.groupby, "time")}, directionkey, infra'
        joinString = ' left join (select distinct pointKey, isdoublereporting, operatorKey, pipeinpipewithtsokey from operatorpointdirections) opd on o.pointKey = opd.pointKey and o.operatorKey = opd.operatorKey'
        if table == physFlowTableName:
            whereString += f' and {checkPipeInPipe}'

        with self.db_accessor() as conn:
            query = f'select {selectString} from "{table}" o {joinString} left join connectionpoints c on o.pointKey=c.pointKey where {whereString} group by {groupString}'
            bil = pd.read_sql_query(query, conn, index_col='time')
        bilanz = bil.pivot(columns=['infra', 'directionkey'])
        bilanz.columns = bilanz.columns.droplevel(None)
        return self._diffHelper(bilanz)

    def _diffHelper(self, df):
        '''
        gets difference for each pair of entry and exit direction index column
        '''
        l = []
        p = pd.DataFrame()
        df.fillna(0, inplace=True)
        for col in df.columns:
            if col[0] not in l:
                for col2 in df.columns:
                    if str(col[0]) == str(col2[0]) and col != col2:
                        # same category
                        l.append(str(col[0]))
                        if col[1] == 'entry':
                            p[str(col[0])] = df[col]-df[col2]
                        else:  # entry - exit
                            p[str(col[0])] = df[col2]-df[col]

                if str(col[0]) not in l:
                    if col[1] == 'entry':
                        p[str(col[0])] = df[col]
                    else:
                        p[str(col[0])] = -df[col]
        return p

    def crossborder(self, operatorKeys: List[str], filt: Filter, group_by: List[str] = ['t.directionkey', 'opd.adjacentzones', 'opd.adjacentcountry'], table=physFlowTableName):
        whereString = timeFilter(filt)
        inString = '("'+'","'.join(operatorKeys)+'")'
        whereString += f'and t.operatorKey in {inString} and {checkDoubleReporting}'

        joinString = ' left join (select distinct pointKey, isdoublereporting, operatorKey, pipeinpipewithtsokey, adjacentzones, adjacentcountry from operatorpointdirections) opd on t.pointKey = opd.pointKey and t.operatorKey = opd.operatorKey'
        if table == physFlowTableName:
            whereString += f' and {checkPipeInPipe}'

        selectString = f'{self.groupTime(filt.groupby, "periodfrom")} as time, '
        selectString += 't.directionkey, adjacentcountry, coalesce(adjacentzones, substr(t.pointKey,0,4)) as adjacentzones, sum(value) as value'
        group_by = ', '.join(list(map(lambda x: ''+x, group_by)))
        groupString = f'{self.groupTime(filt.groupby, "time")}, {group_by}'

        with self.db_accessor() as conn:
            query = f'select {selectString} from "{table}" t {joinString} where {whereString} group by {groupString}'
            flow = pd.read_sql_query(query, conn, index_col='time')

        flow['name'] = flow['adjacentcountry'].apply(lambda x: str(
            x) if x else '-')+':'+flow['adjacentzones'].apply(lambda x: str(x) if x else '-')

        del flow['adjacentcountry']
        del flow['adjacentzones']

        pivoted = flow.pivot(columns=['name', 'directionkey'], values='value')
        return self._diffHelper(pivoted)


if __name__ == "__main__":

    entsog = EntsogSQLite('data/entsog.db')
    operators = entsog.operators()

    start = datetime(2018, 7, 1)
    end = datetime(2018, 7, 22)
    group = 'hour'
    filt = Filter(start, end, group)
    balzones = entsog.balancingzones()
    intercon = entsog.interconnections()
    cpp = entsog.connectionpoints()
    #gen = generation.melt(var_name='kind', value_name='value',ignore_index=False)
    operatorKeys = ['DE-TSO-0004', 'DE-TSO-0007', 'DE-TSO-0005', 'DE-TSO-0006']

    phy = entsog.operationaldata(operatorKeys, filt, group_by=[
                                 'operatorkey', 'directionkey'])
    piv = phy.pivot(columns=['operatorkey', 'directionkey'], values='value')
    piv.plot()

    point = entsog.operationaldataByPoints(
        ['ITP-00043', 'ITP-00111'], Filter(start, end, group), ['pointkey', 'directionkey'])
    point['point'] = point['pointlabel']+' ' + \
        point['directionkey']+' '+point['indicator']
    point['value'] = point['value']/1e6
    piv2 = point.pivot(columns=['point'], values='value')
    piv2.plot()

    end = datetime(2018, 7, 2)
    filt = Filter(start, end, 'hour')
    operatorKeys = entsog.operatorsByBZ('Italy')

    operatorKeys = entsog.operatorsByBZ('Portugal')
    bil = entsog.bilanz(operatorKeys, filt)
    bil.plot(rot=45)

    operatorKeys = entsog.operatorsByBZ('GASPOOL')
    c = entsog.crossborder(operatorKeys, filt)
    c.plot(rot=45)

    # 55 mrd Nm^3 sind 60 GWh
    # 55 000 000 000 = 60 000 000 000 Wh pro Jahr
    # taeglich circa 160 000 000 Wh also 160 MWh
    # tatsaechlich 1 131 141 436 kWh pro Tag?? laut entsog
