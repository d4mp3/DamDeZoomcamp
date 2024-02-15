#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    first_table_name = params.first_table_name
    second_table_name = params.second_table_name
    yellow_tripdata_url = params.yellow_tripdata_url
    zones_url = params.zones_url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if yellow_tripdata_url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata.csv.gz'
    else:
        csv_name = 'yellow_tripdata.csv'

    os.system(f"wget {yellow_tripdata_url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    t_start = time()
    df.head(n=0).to_sql(name=first_table_name, con=engine, if_exists='replace')

    df.to_sql(name=first_table_name, con=engine, if_exists='append')
    t_end = time()
    print('inserted yellow taxi table, took %.3f second' % (t_end - t_start))

    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=first_table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

    # zones table
    csv_name = 'taxi_zones_lookup.csv'
    os.system(f"wget {zones_url} -O {csv_name}")
    df_zones = pd.read_csv(csv_name)
    df_zones.head(n=0).to_sql(name=second_table_name, con=engine, if_exists='replace')
    t_start = time()
    df_zones.to_sql(name=second_table_name, con=engine, if_exists='append')
    t_end = time()
    print('inserted zones table, took %.3f second' % (t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--first_table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--second_table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--yellow_tripdata_url', required=True, help='url of the csv file with yellow taxi data')
    parser.add_argument('--zones_url', required=True, help='url of the csv file with zones data')

    args = parser.parse_args()

    main(args)
