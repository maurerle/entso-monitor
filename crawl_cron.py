#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 18 15:35:12 2021

@author: maurer
"""

from entsoe import EntsoePandasClient
import pandas as pd

from entsoe_data.entsoe_crawler import EntsoeCrawler
from entsog_data.entsog_crawler import EntsogCrawler


def updateEntsoe(db, api_key, first=False):
    client = EntsoePandasClient(api_key=api_key)
    crawler = EntsoeCrawler(database=db)

    if first:
        start = pd.Timestamp('20150101', tz='Europe/Berlin')
        delta = pd.Timestamp.now(tz='Europe/Berlin')-start
        crawler.create_database(client, start, delta)
        crawler.update_database(client, start, delta)
    else:
        crawler.update_database(client)


def updateEntsog(db, first=False):
    crawler = EntsogCrawler(db)

    names = ['cmpUnsuccessfulRequests',
             # 'operationaldata',
             # 'cmpUnavailables',
             # 'cmpAuctions',
             # 'AggregatedData', # operationaldata aggregated for each zone
             # 'tariffssimulations',
             # 'tariffsfulls',
             # 'urgentmarketmessages',
             'connectionpoints',
             'operators',
             'balancingzones',
             'operatorpointdirections',
             'Interconnections',
             'aggregateInterconnections']
    if first:
        crawler.pullData(names)

    indicators = ['Physical Flow', 'Allocation', 'Firm Technical']
    crawler.pullOperationalData(indicators)


if __name__ == '__main__':
    import os
    first = False
    db = os.getenv('DATABASE_URI','postgresql://entso:entso@localhost:5432')
    from sqlalchemy import create_engine

    t = create_engine(f'{db}/entsoe')

    api_key = os.getenv('ENTSOE_API_KEY', 'ae2ed060-c25c-4eea-8ae4-007712f95375')
    updateEntsoe(f'{db}/entsoe', api_key, first=first)
    updateEntsog(f'{db}/entsog', first=first)
