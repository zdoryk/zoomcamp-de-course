#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True)
def transform_data(df):
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    connection_block = SqlAlchemyConnector.load("ny-taxi-docker-postgres")
    with connection_block.get_connection(begin=False) as engine:
        # postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
        # engine = create_engine(postgres_url)
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

    # while True:
    #     try:
    #         t_start = time()
    #
    #         df = next(df_iter)
    #
    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #
    #         df.to_sql(name=table_name, con=engine, if_exists='append')
    #
    #         t_end = time()
    #
    #         print('inserted another chunk, took %.3f second' % (t_end - t_start))
    #
    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break


@flow(name='Sub-flow', log_prints=True)
def log_subflow():
    print('Logging sub flow')


@flow(name="Ingest Flow")
def main_flow(table_name: str):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    raw_data = extract_data(csv_url)
    transformed_data = transform_data(raw_data)
    ingest_data(table_name=table_name, df=transformed_data)


if __name__ == '__main__':
    main_flow("yellow_taxi_trips")
