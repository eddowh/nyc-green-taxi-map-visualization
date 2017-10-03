# -*- coding: utf-8 -*-

from config import TMP_DATA_DIRNAME
from src.utils import reduce_taxi_df_memory_usage

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
