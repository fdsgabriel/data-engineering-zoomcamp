#!/user/bin/env python
# coding: utf-8

import pandas as pd
import pyarrow.parquet as pa
from sqlalchemy import create_engine
import argparse
import os


def main(params):

    #Get parameters
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name

    parquet_name = 'output.parquet'

    #Download parquet
    os.system(f"wget {url} -O {parquet_name}")

    #Creaste a SQL PG engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #Docker has to be on
    engine.connect()

    #Load data
    df_y_taxi = pd.read_parquet(parquet_name, engine='pyarrow')

    ###################################################################################

    #Create the "create" in PG
    df_y_taxi.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    #Insert data indeed
    #get parquet file
    parquet_file = pa.ParquetFile(parquet_name)

    for batch in parquet_file.iter_batches(batch_size=100000):
        df_bacth = batch.to_pandas()  #Converter o batch para um df do pandas
        df_bacth.to_sql(name=table_name, con=engine, if_exists='append')

        print("Next chunk is appending...")


if __name__ == '__main__': 
    parser = argparse.ArgumentParser(description='Ingest data to PG')

    #User
    parser.add_argument('--user', help='user name for PG')

    #password
    parser.add_argument('--password', help='password for PG')

    #host
    parser.add_argument('--host', help='host for PG')

    #port
    parser.add_argument('--port', help='port for PG')

    #database name
    parser.add_argument('--db', help='database name for PG')

    #table name
    parser.add_argument('--table_name', help='table name where the results are going to be written')

    #url of parquet
    parser.add_argument('--url', help='user of parquet file')

    args = parser.parse_args()

    main(args)