# -*- coding: utf-8 -*-

import pandas as pd


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
            df.loc[:, colname] = pd.to_numeric(df.loc[:, colname],
                                               downcast='float')

    do_pu_id_colnames = [
        'dolocationid',
        'pulocationid',
    ]
    if reduce(lambda x, y: x in df.columns and y in df.columns,
              do_pu_id_colnames):
        for colname in do_pu_id_colnames:
            df.loc[:, colname] = df.loc[:, colname].astype('category')

    # categorical variables
    categorical_dtype_colnames = [
        'passenger_count',
        'payment_type',
        'ratecodeid',
        'vendorid',
        'trip_type',
    ]
    for colname in categorical_dtype_colnames:
        df.loc[:, colname] = df.loc[:, colname].astype('category')

    # boolean variables
    if df.loc[:, 'store_and_fwd_flag'].dtype != 'bool':
        df.loc[:, 'store_and_fwd_flag'] = df.loc[
            :, 'store_and_fwd_flag'] == 'Y'

    # datetime variables
    datetime_dtype_colnames = [
        'lpep_dropoff_datetime',
        'lpep_pickup_datetime',
    ]
    for colname in datetime_dtype_colnames:
        df.loc[:, colname] = pd.to_datetime(df.loc[:, colname],
                                            format='%Y-%m-%dT%H:%M:%S.%f')

    return df
