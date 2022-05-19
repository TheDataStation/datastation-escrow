import glob
import os
import pathlib

from dsapplicationregistration import register
from common import utils

import snsql
import pandas as pd

total_epsilon = 1.0

# record the data id for catalog (later we can change the db/storage structure so we don't have to record it here)
catalog_id = 0

cache = {}

# @register()
# def aggregate_data():
#     pass

@register()
def read_catalog(catalog_id = None):
    pass


@register()
def query_sql(sql_query, epsilon=None):
    if epsilon is None:
        epsilon = assign_budget()

    if sql_query in cache.keys():
        return cache[sql_query]

    # assume there is one aggregated dataset to query
    # TODO: get the data and its metadata
    csv_path = 'mock'
    meta_path = 'mock'

    df = pd.read_csv(csv_path)

    privacy = snsql.Privacy(epsilon=epsilon)
    private_reader = snsql.from_connection(df, metadata=meta_path, privacy=privacy)

    cost = private_reader.get_privacy_cost(query_sql)
    budget_spent = private_reader.odometer.spent

    if cost + budget_spent > total_epsilon:
        return "error: budget exceeded"

    result_dp = private_reader.execute_df(sql_query)
    cache[sql_query] = result_dp

    # never reveal the true result
    true_result = private_reader.reader.execute_df(sql_query)

    return result_dp


def assign_budget():
    pass
