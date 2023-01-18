import os

import pandas as pd
from sqlalchemy import create_engine
import sys
import argparse
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    gunzip_filename = url.split('/')[-1]
    os.system(f"wget {url} -O {gunzip_filename} && gunzip {gunzip_filename}")

    csv_name = gunzip_filename[:-3]

    # donload the csv

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()

        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url 0f the csv files')
    args = parser.parse_args()
    main(args)
