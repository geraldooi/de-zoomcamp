#!/usr/bin/env python
# coding: utf-8

import argparse
import numpy as np
import os
import pandas as pd
import pyarrow.parquet as pq

from sqlalchemy import create_engine


def ingest_parquet(params, file_name):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Load parquet file into pandas df
    print('Read parquet file')
    df = pq.read_table(file_name).to_pandas()

    # Slice huge df into smaller chunks
    print('Slice dataframe into smaller chunks')
    df_chunks = np.array_split(df, np.ceil(df.shape[0]/100000))

    # Create table
    print('Create Table')
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=None)

    # Load df into db
    print('Load chunks into table')
    for chunk in df_chunks:
        chunk.to_sql(name=table_name, con=engine, if_exists='append', index=None)

    print('Ingestion complete')


def ingest_csv(params, file_name):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Load parquet file into pandas df
    print('Read parquet file')
    df = pd.read_csv(file_name)

    # Create table
    print('Create Table')
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=None)

    # Load df into db
    print('Load df into table')
    df.to_sql(name=table_name, con=engine, if_exists='append', index=None)

    print('Ingestion complete')


def ingest_data(params):
    url = params.url
    file_name = url.split('/')[-1]

    os.system(f'wget {url} -O {file_name}')

    if file_name.split('.')[-1] == 'parquet':
        ingest_parquet(params, file_name)
    elif file_name.split('.')[-1] == 'csv':
        ingest_csv(params, file_name)
    else:
        print(f'{file_name} is not parquest nor csv file.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Ingest parquet data to Postgres")
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='hostname for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table to write into')
    parser.add_argument('--url', help='url to the parquet file')

    params = parser.parse_args()

    ingest_data(params)
