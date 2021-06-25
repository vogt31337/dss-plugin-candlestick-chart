import dataiku
from flask import request
import pandas as pd
import numpy as np
import json
import traceback
import logging
logger = logging.getLogger(__name__)
from functools import reduce


def numerical_filter(df, filter):
    conditions = []
    if filter["minValue"]:
        conditions += [df[filter['column']] >= filter['minValue']]
    if filter["maxValue"]:
        conditions += [df[filter['column']] <= filter['maxValue']]
    return conditions

 
def alphanum_filter(df, filter):
    conditions = []
    excluded_values = []
    for k, v in filter['excludedValues'].items():
        if k != '___dku_no_value___':
            if v:
                excluded_values += [k]
        else:
            if v:
                conditions += [~df[filter['column']].isnull()]
    if len(excluded_values) > 0:
        if filter['columnType'] == 'NUMERICAL':
            excluded_values = [float(x) for x in excluded_values]
        conditions += [~df[filter['column']].isin(excluded_values)]
    return conditions


def date_filter(df, filter):
    if filter["dateFilterType"] == "RANGE":
        return date_range_filter(df, filter)
    else:
        return special_date_filter(df, filter)


def date_range_filter(df, filter):
    conditions = []
    if filter["minValue"]:
        conditions += [df[filter['column']] >= pd.Timestamp(filter['minValue'], unit='ms')]
    if filter["maxValue"]:
        conditions += [df[filter['column']] <= pd.Timestamp(filter['maxValue'], unit='ms')]
    return conditions


def special_date_filter(df, filter):
    conditions = []
    excluded_values = []
    for k, v in filter['excludedValues'].items():
        if v:
            excluded_values += [k]
    if len(excluded_values) > 0:
        if filter["dateFilterType"] == "YEAR":
            conditions += [~df[filter['column']].dt.year.isin(excluded_values)]
        elif filter["dateFilterType"] == "QUARTER_OF_YEAR":
            conditions += [~df[filter['column']].dt.quarter.isin([int(k)+1 for k in excluded_values])]
        elif filter["dateFilterType"] == "MONTH_OF_YEAR":
            conditions += [~df[filter['column']].dt.month.isin([int(k)+1 for k in excluded_values])]
        elif filter["dateFilterType"] == "WEEK_OF_YEAR":
            conditions += [~df[filter['column']].dt.week.isin([int(k)+1 for k in excluded_values])]
        elif filter["dateFilterType"] == "DAY_OF_MONTH":
            conditions += [~df[filter['column']].dt.day.isin([int(k)+1 for k in excluded_values])]
        elif filter["dateFilterType"] == "DAY_OF_WEEK":
            conditions += [~df[filter['column']].dt.dayofweek.isin(excluded_values)]
        elif filter["dateFilterType"] == "HOUR_OF_DAY":
            conditions += [~df[filter['column']].dt.hour.isin(excluded_values)]
        else:
            raise Exception("Unknown date filter.")

    return conditions


def apply_filter_conditions(df, conditions):
    """
    return a function to apply filtering conditions on df
    """
    if len(conditions) == 0:
        return df
    elif len(conditions) == 1:
        return df[conditions[0]]
    else:
        return df[reduce(lambda c1, c2: c1 & c2, conditions[1:], conditions[0])]

             
def filter_dataframe(df, filters):
    """
    return the input dataframe df with filters applied to it
    """
    conditions = []
    for filter in filters:
        try:
            if filter["filterType"] == "NUMERICAL_FACET":
                df = apply_filter_conditions(df, numerical_filter(df, filter))
            elif filter["filterType"] == "ALPHANUM_FACET":
                df = apply_filter_conditions(df, alphanum_filter(df, filter))
            elif filter["filterType"] == "DATE_FACET":
                df = apply_filter_conditions(df, date_filter(df, filter))
        except Exception as e:
            raise Exception("Error with filter on column {} - {}".format(filter["column"], e))
    if df.empty:
        raise Exception("Dataframe is empty after filtering")
    return df


def create_candlestick_df(df, category_column, value_column, max_displayed_values, group_others):
    """
    return a dataframe in the format expected for google candlestick charts after performing the aggregation and sum of categories
    only $max_displayed_values categories are kept and others are merged into one if $group_others
    """
    try:    # groupby per categories and get count and sum of values
        df = df.groupby([category_column], as_index=False).agg({value_column:['sum','count']})
        df.columns = [category_column, value_column+'_sum', value_column+'_count']
    except:
        raise TypeError("Cannot perform Groupby for column {}".format(category_column))

    n = df.shape[0]
    if n > max_displayed_values:  # aggregate small categories into one (keep only the n biggest categories in term of count)
        # sort categories according to their count
        df = df.sort_values(by=[value_column+'_count'], ascending=False).reset_index(drop=True)
        df['rank'] = df.index
        # all categories with rank higher than max displayed values get the same rank
        df['rank'] = df.apply(lambda row: row['rank'] if row['rank'] < max_displayed_values-1 else max_displayed_values-1, axis=1)
        if group_others: # if small categories are to be grouped into one, then create a 'others' category
            df[category_column] = df.apply(lambda row: row[category_column] if row['rank'] < max_displayed_values-1 else 'others', axis=1)
            df = df.groupby(['rank'], as_index=False).agg({category_column:'min', value_column+'_sum':'sum'})
        else:
            df = df.head(max_displayed_values)

    df = df[[category_column, value_column+'_sum']]  # keep only the two columns needed
    df.columns = [category_column, value_column]  # rename value_coluln_sum with value_column

    df = df.sort_values(by=[value_column], ascending=False).reset_index(drop=True)

    # add columns to transform the dataframe into the format expected for a google candlestick chart
    df['max'] = df[value_column]
    df[value_column] = df[value_column].shift(1)
    df.loc[0, value_column] = 0
    df[[value_column, 'max']] = df[[value_column, 'max']].cumsum()

    return df

@app.route('/reformat_data')
def reformat_data():
    try:
        config = json.loads(request.args.get('config', None))
        filters = json.loads(request.args.get('filters', None))

        dataset_name = config.get('dataset_name')
        category_column = config.get('category_column')
        value_column = config.get('value_column')
        max_displayed_values = int(config.get('max_displayed_values'))
        group_others = config.get('group_others')

        df = dataiku.Dataset(dataset_name).get_dataframe()
        if df.empty:
            raise Exception("Dataframe is empty")

        if df[value_column].dtype not in [np.dtype(int), np.dtype(float), np.dtype("float32")]:
            raise TypeError("Values must be of numerical types")

        if len(filters) > 0:  # apply filters to dataframe
            df = filter_dataframe(df, filters)

        columns_list = [x for x in [category_column, value_column] if x is not None]
        df = df[columns_list]   # only keep the necessary columns

        df = create_candlestick_df(df, category_column, value_column, max_displayed_values, group_others)

        return json.dumps({'result': df.values.tolist()})
    except Exception as e:
        logger.error(traceback.format_exc())
        return str(e), 500
