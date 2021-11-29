#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 18:53:14 2020

@author: maurer
"""
from tqdm import tqdm
from contextlib import closing
import sqlite3 as sql
import pandas as pd
from datetime import timedelta
import time
from sqlalchemy import create_engine
from contextlib import contextmanager

from entsoe import EntsoePandasClient
from entsoe.mappings import PSRTYPE_MAPPINGS, NEIGHBOURS, Area
from entsoe.exceptions import NoMatchingDataError
from requests.exceptions import HTTPError

import logging

logging.basicConfig()
log = logging.getLogger('entsoe')
log.setLevel(logging.INFO)

from_n = []
to_n = []

for n1 in NEIGHBOURS:
    for n2 in NEIGHBOURS[n1]:
        from_n.append(n1)
        to_n.append(n2)
neighbours = pd.DataFrame({'from': from_n, 'to': to_n})


def replaceStr(string):
    '''
    replaces illegal values from a spark series name
    '''

    st = str.replace(string, ')', '')
    st = str.replace(st, '(', '')
    st = str.replace(st, ',', '')
    st = str.replace(st, "'", '')
    st = st.strip()
    st = str.replace(st, ' ', '_')
    return st


def calcDiff(data, inplace=False):
    if not inplace:
        dat = data.copy()
    else:
        dat = data
    for c in filter(lambda x: x.endswith('_Actual_Aggregated'), dat.columns):
        new = str.replace(c, '_Actual_Aggregated', '')
        dif = list(filter(lambda x: x.endswith('_Actual_Consumption')
                          and x.startswith(new), dat.columns))
        if len(dif) > 0:
            # wenn es beides gibt wird die Differenz gebildet
            dat[new] = dat[c]-dat[dif[0]]
            del dat[c]
            del dat[dif[0]]
        else:
            # sonst wird direkt
            dat[new] = dat[c]
            del dat[c]
    for c in filter(lambda x: x.endswith('_Actual_Consumption'), dat.columns):
        # wenn es nur Verbrauch aber kein Erzeugnis gibt, mach negativ
        new = str.replace(c, '_Actual_Consumption', '')
        dat[new] = -dat[c]
        del dat[c]
    return dat


class EntsoeCrawler:
    def __init__(self, folder, spark=None, database=None):
        self.spark = spark
        self.folder = folder
        if database:
            if database.startswith('postgresql'):

                self.engine = create_engine(database)
                @contextmanager
                def access_db():
                    yield self.engine

                self.db_accessor = access_db
            else:
                self.db_accessor = lambda: closing(sql.connect(database))
        else:
            self.db_accessor = None

    def initBaseSql(self):
        psrtype = pd.DataFrame.from_dict(
            PSRTYPE_MAPPINGS, orient='index', columns=['prod_type'])
        areas = pd.DataFrame([[e.name, e.value, e.tz, e.meaning]
                              for e in Area], columns=['name', 'value', 'tz', 'meaning'])
        with self.db_accessor() as conn:
            areas.columns = [x.lower() for x in areas.columns]
            psrtype.columns = [x.lower() for x in psrtype.columns]
            areas.to_sql('areas', conn, if_exists='replace')
            psrtype.to_sql('psrtype', conn, if_exists='replace')

    def pullData(self, procedure, country_code, start, end):
        data = pd.DataFrame(procedure(country_code, start=start, end=end))
        return data

    def persist(self, country, proc, start, end):
        try:
            try:
                data = self.pullData(proc, country, start, end)
            except NoMatchingDataError as e:
                raise e
            except HTTPError as e:
                log.error(e.response.status_code, e.response.reason)
                if e.response.status_code == 400:
                    raise e
                else:
                    log.info(f'retrying: {repr(e)}, {start}, {end}')
                    time.sleep(10)
                    data = self.pullData(proc, country, start, end)

            except Exception as e:
                log.info(f'retrying: {repr(e)}, {start}, {end}')
                time.sleep(10)
                data = self.pullData(proc, country, start, end)

            # replace spaces and invalid chars in column names
            data.columns = [replaceStr(x).lower() for x in data.columns]
            data.fillna(0, inplace=True)
            # calculate difference betweeen agg and consumption
            data = calcDiff(data, inplace=True)
            # add country column
            data['country'] = country
            if self.db_accessor:
                with self.db_accessor() as conn:
                    try:
                        data.to_sql(proc.__name__, conn, if_exists='append')
                    except Exception as e:
                        log.info(f'handling {repr(e)} by concat')
                        # merge old data with new data
                        prev = pd.read_sql_query(
                            f'select * from {proc.__name__}', conn, index_col='index')
                        dat = pd.concat([prev, data])
                        # convert type as pandas needs it
                        dat.index = dat.index.astype('datetime64[ns]')
                        dat.to_sql(proc.__name__, conn, if_exists='replace')
                        log.info(f'replaced table {proc.__name__}')

            if self.spark != None:
                data['time'] = data.index
                spark_data = self.spark.createDataFrame(data)

                #new_names = list(map(replaceStr, spark_data.schema.fieldNames()))
                #spark_data = spark_data.toDF(*new_names)
                spark_data.write.mode('append').parquet(
                    f'{self.folder}/{country}/{proc.__name__}')
        except NoMatchingDataError:
            log.error('no data found for {proc.__name__}, {start}, {end}')
        except Exception as e:
            log.exception(f'error downloading {start}, {end}')

    def bulkDownload(self, countries, procs, start, delta, times):
        end = start+delta
        for proc in procs:
            # hier könnte man parallelisieren
            for country in countries:
                log.info('')
                log.info(f'{country}, {proc.__name__}')
                pbar = tqdm(range(times))
                for i in pbar:
                    start1 = start + i * delta
                    end1 = end + i*delta

                    pbar.set_description(
                        f"{country} {start1:%Y-%m-%d} to {end1:%Y-%m-%d}")

                    self.persist(country, proc, start1, end1)

            with self.db_accessor() as conn:
                log.info(f'creating index country_idx_{proc.__name__}')
                query = (
                    f'CREATE INDEX IF NOT EXISTS "country_idx_{proc.__name__}" ON "{proc.__name__}" ("country", "index");')
                conn.execute(query)
                #query = (f'CREATE INDEX IF NOT EXISTS "country_{proc.__name__}" ON "{proc.__name__}" ("country");')
                # conn.execute(query)
                log.info(f'created indexes country_idx_{proc.__name__}')

    def pullCrossborders(self, start, delta, times, proc, allZones=True):
        end = start+delta
        for i in range(times):
            data = pd.DataFrame()
            start1 = start + i * delta
            end1 = end + i*delta
            log.info(start1)

            for n1 in NEIGHBOURS:
                for n2 in NEIGHBOURS[n1]:
                    try:
                        if (len(n1) == 2 and len(n2) == 2) or allZones:
                            dataN = proc(n1, n2, start=start1, end=end1)
                            data[n1+'-'+n2] = dataN
                    except NoMatchingDataError:
                        #log.info('no data found for ',n1,n2)
                        pass
                    except Exception:
                        log.exception('Error crawling Crossboarders')

            data.columns = [x.lower() for x in data.columns]
            if self.db_accessor:
                with self.db_accessor() as conn:
                    try:
                        data.to_sql(proc.__name__, conn, if_exists='append')
                    except Exception:
                        log.exception('error saving crossboarders')
                        prev = pd.read_sql_query(
                            f'select * from {proc.__name__}', conn, index_col='index')

                        ges = pd.concat([prev, data])
                        ges.index = ges.index.astype('datetime64[ns]')
                        ges.to_sql(proc.__name__, conn, if_exists='replace')

            if self.spark != None:
                data['time'] = data.index
                spark_data = self.spark.createDataFrame(data)
                spark_data.write.mode('append').parquet(
                    f'{self.folder}/{proc.__name__}')

    def pullPowerSystemData(self):
        df = pd.read_csv(
            'https://data.open-power-system-data.org/conventional_power_plants/latest/conventional_power_plants_EU.csv')
        df.dropna(axis=0, subset=['lon', 'lat', 'eic_code'], inplace=True)
        df = df[['eic_code', 'name', 'company', 'country',
                 'capacity', 'energy_source', 'lon', 'lat']]
        # delete those without location or eic_code

        if self.db_accessor:
            with self.db_accessor() as conn:
                df.to_sql('powersystemdata', conn, if_exists='replace')

        if self.spark != None:
            df.to_parquet(f'{self.folder}/powersystemdata')
            #spark_data = spark.createDataFrame(df)
            # spark_data.write.mode('append').parquet(f'{self.folder}/powersystemdata')
        return df

    def bulkDownloadPlantData(self, countries, client, start, delta, times):
        # new proxy function
        def query_per_plant(country, start, end):
            ppp = client.query_generation_per_plant(
                country, start=start, end=end)
            # convert multiindex into second column
            pp = ppp.melt(var_name=['name', 'type'],
                          value_name='value', ignore_index=False)
            # modify encoding to utf-8, upstream fix contributed
            #pp['name'] = pp['name'].str.encode('latin-1').str.decode('utf-8')
            return pp

        procs = [query_per_plant]
        self.bulkDownload(countries, procs, start, delta=delta, times=times)

        with self.db_accessor() as conn:
            query = 'CREATE INDEX IF NOT EXISTS "idx_name_query_per_plant" ON "query_per_plant" ("name", "index", "country");'
            conn.execute(query)

        with self.db_accessor() as conn:
            query = 'select distinct name, country,type from query_per_plant'
            names = pd.read_sql_query(query, conn)
            names.to_sql('plant_names', conn, if_exists='replace')

    def plantCountries(self, client):
        plant_countries = []
        st = pd.Timestamp('20180101', tz='Europe/Berlin')
        for country in [e.name for e in Area]:
            try:
                _ = client.query_generation_per_plant(
                    country, start=st, end=st+timedelta(days=1))
                plant_countries.append(country)
                log.info(f'found data for {country}')
            except Exception:
                continue
        return plant_countries

    def updateDatabase(self, client, start=None, delta=None):

        if not (start and delta):
            import pytz
            try:
                with self.db_accessor() as conn:
                    query = 'select max("index") from query_day_ahead_prices'
                    d = conn.execute(query).fetchone()[0]
                start = pd.to_datetime(d)
            except Exception:
                start = pd.Timestamp('20150101', tz=pytz.FixedOffset(120))

            end = pd.Timestamp.now(tz=pytz.FixedOffset(120))
            delta = end-start
        else:
            end = start+delta
        countries = [e.name for e in Area]

        if end.year-start.year > 0:
            gen_procs = [client.query_installed_generation_capacity,
                         client.query_installed_generation_capacity_per_unit]
            self.bulkDownload(countries, gen_procs,
                              start, delta=delta, times=1)

        # timeseries
        ts_procs = [client.query_day_ahead_prices,
                    client.query_load,
                    client.query_load_forecast,
                    client.query_generation_forecast,
                    client.query_wind_and_solar_forecast,
                    client.query_generation]

        # Download load and generation
        self.bulkDownload(countries, ts_procs, start, delta, times=1)

        self.pullCrossborders(start, delta, 1, client.query_crossborder_flows)

        plant_countries = self.plantCountries(client)
        self.bulkDownloadPlantData(
            plant_countries[:], client, start, delta, times=1)

    def createDatabase(self, client, start, delta):
        self.initBaseSql()
        self.pullPowerSystemData()
        self.updateDatabase(client, start, delta)


if __name__ == "__main__":
    # Create a spark session
    from pyspark.sql import SparkSession
    from pyspark import SparkConf
    import findspark
    findspark.init()
    conf = SparkConf().setAppName('entsoe').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    log.info('')
    log.info('ENTSOE')

    client = EntsoePandasClient(api_key='ae2ed060-c25c-4eea-8ae4-007712f95375')

    start = pd.Timestamp('20150101', tz='Europe/Berlin')
    delta = timedelta(days=30)
    end = start+delta

    times = 6*12  # bis 2021

    entsoe_path = 'hdfs://149.201.206.53:9000/user/fmaurer/entsoe'
    db = 'data/entsoe.db'

    crawler = EntsoeCrawler(folder='data/spark', spark=spark, database=db)
    procs = [client.query_day_ahead_prices,
             client.query_load,
             client.query_load_forecast,
             client.query_generation_forecast,
             client.query_wind_and_solar_forecast,
             client.query_generation]

    countries = [e.name for e in Area]
    # Download load and generation
    crawler.bulkDownload(countries, procs, start, delta, times)

    # Capacities

    def downloadCapPerUnit(country, start, end):
        pp = client.query_installed_generation_capacity_per_unit(
            country, start=start, end=end)
        # modify encoding to utf-8, upstream fix contributed
        #pp['Name'] = pp['Name'].str.encode('latin-1').str.decode('utf-8')
        return pp

    procs = [client.query_installed_generation_capacity,
             client.query_installed_generation_capacity_per_unit]

    # crawler.bulkDownload(countries,procs,start,delta=timedelta(days=365*6),times=1)
    # crawler.pullPowerSystemData()
    # crawler.bulkDownload(countries,[client.query_installed_generation_capacity_per_unit],start,delta=timedelta(days=360*6),times=1)

    # Crossborder Data
    # s tart = pd.Timestamp('20181211', tz='Europe/Berlin')
    # 2018-12-11 00:00:00+01:00 fehlt 1 mal, database locked

    # crawler.pullCrossborders(start,delta,1,client.query_crossborder_flows)

    # per plant generation
    plant_countries = []
    st = pd.Timestamp('20180101', tz='Europe/Berlin')

    for country in countries:
        try:
            test_data = client.query_generation_per_plant(
                country, start=st, end=st+timedelta(days=1))
            plant_countries.append(country)
            log.info(f'found data for {country}')
        except Exception:
            continue

    db = 'data/entsoe.db'
    crawler = EntsoeCrawler(folder='data/spark', spark=None, database=db)

    # 2017-12-16 bis 2018-03-15 runterladen
    crawler.bulkDownloadPlantData(
        plant_countries[:], client, start, delta, times)

    # create indices if not existing