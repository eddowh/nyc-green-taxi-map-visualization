# -*- coding: utf-8 -*-

from config import TMP_DATA_DIRNAME

from concurrent.futures import ThreadPoolExecutor
from sodapy import Socrata
from functools import reduce
import asyncio
import feather
import functools
import math
import os
import pandas as pd

DATASET_ID = "pqfs-mqru"
TOTAL_ROWS = 16385532
SOURCE_DOMAIN = "data.cityofnewyork.us"
API_VERSION = 2.1

# XXX: obfuscate this app token
APP_TOKEN = "DjZ1QvFkXs29oV7hayqW08BVk"
TIMEOUT = 120

# configure batch size
# NOTE: the max number of file descriptors is 1024
NUM_TASKS = 5
BATCH_SIZE = 20000

# `data/tmp/<limit>/<offset>`
# limit == BATCH_SIZE
TMP_DATA_SUBDIRNAME = os.path.join(TMP_DATA_DIRNAME, str(BATCH_SIZE))


def reduce_taxi_df_memory_usage(df):
    # float variables
    float_dtype_colnames = [
        'extra',
        'fare_amount',
        'improvement_surcharge',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'total_amount',
        'trip_distance',
    ]
    for colname in float_dtype_colnames:
        df[colname] = pd.to_numeric(df[colname], downcast='float')

    # drop off / pick-up can be specified as `dolocationid` and `pulocationid`
    # instead of geographical coordinates
    do_pu_longlat_colnames = [
        'dropoff_latitude',
        'dropoff_longitude',
        'pickup_latitude',
        'pickup_longitude',
    ]
    if reduce(lambda x, y: x in df.columns and y in df.columns,
              do_pu_longlat_colnames):
        for colname in do_pu_colnames:
            df[colname] = pd.to_numeric(df[colname], downcast='float')

    do_pu_id_colnames = [
        'dolocationid',
        'pulocationid',
    ]
    if reduce(lambda x, y: x in df.columns and y in df.columns,
              do_pu_id_colnames):
        for colname in do_pu_id_colnames:
            df[colname] = df[colname].astype('category')

    # categorical variables
    categorical_dtype_colnames = [
        'passenger_count',
        'payment_type',
        'ratecodeid',
        'vendorid',
        'trip_type',
    ]
    for colname in categorical_dtype_colnames:
        df[colname] = df[colname].astype('category')

    # boolean variables
    df.store_and_fwd_flag = df.store_and_fwd_flag == 'Y'

    # datetime variables
    datetime_dtype_colnames = [
        'lpep_dropoff_datetime',
        'lpep_pickup_datetime',
    ]
    for colname in datetime_dtype_colnames:
        df[colname] = pd.to_datetime(df[colname],
                                     format='%Y-%m-%dT%H:%M:%S.%f')

    return df


def get_taxi_data(**kwargs):
    while True:
        client = Socrata(SOURCE_DOMAIN, app_token=APP_TOKEN, timeout=TIMEOUT)
        try:
            results = client.get(DATASET_ID, **kwargs)
            break
        except Exception:
            pass
    return results


def write_taxi_df(**kwargs):
    results = get_taxi_data(**kwargs)
    # convert to dataframe
    df = reduce_taxi_df_memory_usage(pd.DataFrame.from_records(results))
    # write into the `data/tmp/<limit>` folder, with the offset as the filename
    # i.e. 'data/tmp/<limit>/<offset>.feather
    feather.write_dataframe(
        df, os.path.join(TMP_DATA_SUBDIRNAME, "{}.feather".format(
            str(kwargs['offset']).zfill(len(str(TOTAL_ROWS))))
        )
    )

async def write_taxi_df_async(loop, **kwargs):
    with ThreadPoolExecutor(max_workers=100) as executor:
        await loop.run_in_executor(
            executor, functools.partial(write_taxi_df, **kwargs)
        )
    print("Write {}: COMPLETE".format(kwargs))


def main():
    # create 'data/tmp/<limit>' folder
    if not os.path.exists(TMP_DATA_SUBDIRNAME):
        os.makedirs(TMP_DATA_SUBDIRNAME)
    # asynchronous tasks
    ioloop = asyncio.get_event_loop()
    for n in range(NUM_TASKS):
        print("Starting task batch {}".format(n + 1))
        subtasks = [
            ioloop.create_task(
                write_taxi_df_async(ioloop, limit=BATCH_SIZE,
                                    offset=i * BATCH_SIZE)
            )
            for i in range(
                n * math.ceil(TOTAL_ROWS / BATCH_SIZE) // NUM_TASKS,
                (n + 1) * math.ceil(TOTAL_ROWS / BATCH_SIZE) // NUM_TASKS,
            )
        ]
        ioloop.run_until_complete(asyncio.wait(subtasks))
    ioloop.close()

if __name__ == '__main__':
    main()
