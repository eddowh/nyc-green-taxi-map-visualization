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
# NOTE: the max number of file descriptors is 1024
BATCH_SIZE = math.ceil(TOTAL_ROWS / 768)


def get_taxi_data(**kwargs):
    while True:
        client = Socrata(SOURCE_DOMAIN, app_token=APP_TOKEN, timeout=TIMEOUT)
        try:
            results = client.get(DATASET_ID, **kwargs)
            break
        except Exception:
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
    task_batch_size = 4
    for n in range(task_batch_size):
        print("Starting task batch {}".format(n + 1))
        tasks = [
            ioloop.create_task(
                get_taxi_data_async(ioloop, limit=BATCH_SIZE,
                                    offset=i * BATCH_SIZE)
            )
            for i in range(
                n * math.ceil(TOTAL_ROWS / BATCH_SIZE) // task_batch_size,
                (n + 1) * math.ceil(TOTAL_ROWS / BATCH_SIZE) // task_batch_size,
            )
        ]
        ioloop.run_until_complete(asyncio.wait(tasks))
    ioloop.close()

if __name__ == '__main__':
    main()
