#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 18 15:35:12 2021

@author: maurer
"""

from entsoe import EntsoePandasClient
import pandas as pd

from entsoe_vis.entsoe_crawler import EntsoeCrawler
from entsog.entsog_crawler import EntsogCrawler


def updateEntsoe(db, first=False):
    client = EntsoePandasClient(api_key='ae2ed060-c25c-4eea-8ae4-007712f95375')
    crawler = EntsoeCrawler(folder='data/spark', spark=None, database=db)

    if first:
        start = pd.Timestamp('20150101', tz='Europe/Berlin')
        delta = pd.Timestamp.now(tz='Europe/Berlin')-start
        crawler.createDatabase(client, start, delta)
    else:
        crawler.updateDatabase(client)


def updateEntsog(db, first=False):
    crawler = EntsogCrawler(db, sparkfolder=None)

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
    # updateEntsoe('data/entsoe.db',first=False)
    # updateEntsog('data/entsog.db',first=False)
    db = 'postgresql://entso:entso@10.13.10.41:5432'
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    t = create_engine(f'{db}/entsoe')

    updateEntsoe(f'{db}/entsoe', first=True)
    updateEntsog(f'{db}/entsog', first=True)