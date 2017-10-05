# -*- coding: utf-8 -*-

import feather
import pandas as pd
from glob import glob

from src.utils import reduce_taxi_df_memory_usage

feather_files = glob('data/tmp/20000/*.feather')


def get_concatenated_df():
    sub_dataframes = list(
        map(reduce_taxi_df_memory_usage,
            map(feather.read_dataframe, feather_files))
    )
    df = reduce_taxi_df_memory_usage(
        pd.concat(sub_dataframes, axis=0)
    )
    return df


def main():
    df = get_concatenated_df()

if __name__ == '__main__':
    main()
