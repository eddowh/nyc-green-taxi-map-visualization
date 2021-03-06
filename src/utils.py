# -*- coding: utf-8 -*-

import pandas as pd

from functools import reduce


def reduce_taxi_df_memory_usage(df):
    """
    Reduce memory footprint of the taxi data.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe that will have its memory footprint reduced.

    Returns
    -------
    df : pandas.DataFrame
        The original array with the reduced memory footprint.
    """
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
        if df.loc[:, colname].dtype != 'float32':
            df.loc[:, colname] = pd.to_numeric(df.loc[:, colname],
                                               downcast='float')

    # drop off / pick-up can be specified as `dolocationid` and `pulocationid`
    # instead of geographical coordinates
    do_pu_longlat_colnames = [
        'dropoff_latitude',
        'dropoff_longitude',
        'pickup_latitude',
        'pickup_longitude',
    ]
    if reduce(lambda a, b: a and b,
              map(lambda c: c in df.columns, do_pu_longlat_colnames)):
        for colname in do_pu_longlat_colnames:
            if df.loc[:, colname].dtype != 'float32':
                df.loc[:, colname] = pd.to_numeric(df.loc[:, colname],
                                                   downcast='float')

    do_pu_id_colnames = [
        'dolocationid',
        'pulocationid',
    ]
    if reduce(lambda x, y: x in df.columns and y in df.columns,
              do_pu_id_colnames):
        for colname in do_pu_id_colnames:
            try:
                is_category = df.loc[:, colname].dtype == 'category'
            except TypeError:
                is_category = False
            finally:
                if not is_category:
                    df.loc[:, colname] = df.loc[:, colname] \
                        .fillna(0) \
                        .astype('category', ordered=True)

    # categorical variables
    categorical_dtype_colnames = [
        'passenger_count',
        'payment_type',
        'ratecodeid',
        'vendorid',
        'trip_type',
    ]
    for colname in categorical_dtype_colnames:
        try:
            is_category = df.loc[:, colname].dtype == 'category'
        except TypeError:
            is_category = False
        finally:
            if not is_category:
                df.loc[:, colname] = df.loc[:, colname] \
                    .fillna(0) \
                    .astype('category', ordered=True)

    # boolean variables
    if df.loc[:, 'store_and_fwd_flag'].dtype != 'bool':
        df.loc[:, 'store_and_fwd_flag'] = \
            df.loc[:, 'store_and_fwd_flag'] == 'Y'

    # datetime variables
    datetime_dtype_colnames = [
        'lpep_dropoff_datetime',
        'lpep_pickup_datetime',
    ]
    for colname in datetime_dtype_colnames:
        if df.loc[:, colname].dtype != 'datetime64[ns]':
            df.loc[:, colname] = pd.to_datetime(df.loc[:, colname],
                                                format='%Y-%m-%dT%H:%M:%S.%f')

    return df


def mem_usage(pandas_obj):
    """
    Displays memory usage of a dataframe or series.

    NOTE: not authored by me (Eddo W. Hintoso).

    Courtesy of Josh Devlin from DataQuest.
    Source: <https://www.dataquest.io/blog/pandas-big-data/>

    Parameters
    ----------
    pandas_obj : pandas.DataFrame, pandas.Series
        The pandas object (either a DataFrame or Series) that we will
        calculate the memory usage of.

    Returns
    -------
    usage_mb : str
        The string representation of the memory usage in megabytes (MB).
    """
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)
