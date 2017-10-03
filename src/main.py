# -*- coding: utf-8 -*-

import math
import pandas as pd
from sodapy import Socrata
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools

DATASET_ID = "pqfs-mqru"
TOTAL_ROWS = 16385532
SOURCE_DOMAIN = "data.cityofnewyork.us"
API_VERSION = 2.1

# XXX: obfuscate this app token
APP_TOKEN = "DjZ1QvFkXs29oV7hayqW08BVk"
TIMEOUT = 120

# configure batch size
BATCH_SIZE = 5000


def get_taxi_data(**kwargs):
    while True:
        client = Socrata(SOURCE_DOMAIN, app_token=APP_TOKEN, timeout=TIMEOUT)
        try:
            results = client.get(DATASET_ID, **kwargs)
            break
        except ConnectionError:
            pass
    return results

async def get_taxi_data_async(loop, **kwargs):
    with ThreadPoolExecutor(max_workers=100) as executor:
        results = await loop.run_in_executor(
            executor, functools.partial(get_taxi_data, **kwargs)
        )
    print("Task with args {} complete!".format(kwargs))
    return results


def main():
    ioloop = asyncio.get_event_loop()
    tasks = [
        ioloop.create_task(
            get_taxi_data_async(ioloop, limit=BATCH_SIZE,
                                offset=i * BATCH_SIZE)
        )
        for i in range(math.ceil(TOTAL_ROWS / BATCH_SIZE))
    ]
    ioloop.run_until_complete(asyncio.wait(tasks))
    ioloop.close()

if __name__ == '__main__':
    main()
