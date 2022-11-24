import glob
import os
import pathlib
from io import BytesIO

import yaml

from dsapplicationregistration import register
from common import utils

import snsql
import pandas as pd
import pprint
import time
import numpy as np
from integration_tests.private_query.find_epsilon import *

# total_epsilon = 1.0

# parameters for finding epsilon
risk_group_percentile = -1
risk_group_size = 100
eps_list = list(np.arange(0.001, 0.01, 0.001, dtype=float))
eps_list += list(np.arange(0.01, 0.11, 0.01, dtype=float))
eps_list += list(np.arange(0.1, 1.1, 0.1, dtype=float))
eps_list += list(np.arange(1, 11, 1, dtype=float))
num_runs = 5
q = 0.05
test = "mw"

@register()
def aggregate_data(context):
    # combines all csv files in storage (that are accessible) and returns the combined dataframe

    files = utils.get_all_files()
    df_to_combine = []
    for file in files:
        if pathlib.Path(file).suffix == ".csv":
            csv_path = pathlib.Path(file).absolute()
            df = pd.read_csv(csv_path)
            df_to_combine.append(df)
    combined_df = pd.concat(df_to_combine)

    return combined_df


@register()
def read_catalog(context, catalog_id=None):
    # return the schema information for each csv file in storage (that are accessible)

    files = utils.get_all_files()
    schema_dict = {}
    for file in files:
        if pathlib.Path(file).suffix == ".csv":
            csv_path = pathlib.Path(file).absolute()
            df = pd.read_csv(csv_path)
            schema = df.columns.values.tolist()
            schema_dict[file] = schema

    return schema_dict


@register()
def private_query_sql(context, sql_query, file_to_query, epsilon=None):

    files = utils.get_all_files()
    if file_to_query not in files:
        print(f"{file_to_query} not accessible")
        return None

    if "budget_spent" not in context:
        context["budget_spent"] = 0.0

    df = pd.read_csv(file_to_query)

    if epsilon is None:

        min_eps = context["budget_spent"]
        cur_eps_to_test = [eps for eps in eps_list if eps >= min_eps]
        if min_eps != 0.0 and min_eps not in cur_eps_to_test:
            cur_eps_to_test.append(min_eps)

        best_eps, dp_result = find_epsilon(df, sql_query, risk_group_percentile, risk_group_size, cur_eps_to_test, num_runs, q)

        if best_eps is None:
            print("cannot find epsilon")
            return None

        context["budget_spent"] += best_eps

    else:

        table_names = extract_table_names(sql_query)
        table_name = table_names.pop()

        metadata = get_metadata(df, table_name)

        query_string = re.sub(f" FROM {table_name}", f" FROM {table_name}.{table_name}", sql_query,
                              flags=re.IGNORECASE)

        privacy = Privacy(epsilon=epsilon)
        reader = snsql.from_df(df, privacy=privacy, metadata=metadata)
        # reader._options.censor_dims = False
        # reader._refresh_options()

        dp_result = reader.execute_df(query_string)

        context["budget_spent"] += epsilon

    print("budget spent =", str(context["budget_spent"]))

    return dp_result


# @register()
# def private_query_sql(context, sql_query, file_to_query, epsilon):
#     files = utils.get_all_files()
#     if file_to_query not in files:
#         return f"{file_to_query} not accessible"
#
#     # if sql_query in cache.keys():
#     #     return cache[sql_query]
#
#     df = pd.read_csv(file_to_query)
#     # df = df.convert_dtypes()
#
#     # generate the metadata required by smartnoise automatically
#     name = pathlib.Path(file_to_query).stem
#     metadata = get_metadata(df, name)
#     # print("metadata:")
#     # pprint.pprint(metadata)
#
#     privacy = snsql.Privacy(epsilon=epsilon)
#     private_reader = snsql.from_connection(df, metadata=metadata, privacy=privacy)
#
#     eps_cost, delta_cost = private_reader.get_privacy_cost(sql_query)
#     # eps_spent, delta_spent = private_reader.odometer.spent
#
#     if "budget_spent" not in context:
#         context["budget_spent"] = 0.0
#     budget_spent = context["budget_spent"]
#
#     if eps_cost + budget_spent > total_epsilon:
#         print(f"error: budget exceeded. total budget = {total_epsilon}",
#               f"budget spent = {budget_spent}",
#               f"epsilon cost = {eps_cost}")
#         return None
#
#     context["budget_spent"] = budget_spent + eps_cost
#     print(f"total budget = {total_epsilon}",
#           f"budget spent = {budget_spent}",
#           f"epsilon cost = {eps_cost}")
#
#     start_time = time.time()
#     result_dp = private_reader.execute_df(sql_query)
#     elapsed = time.time() - start_time
#     print(f"actual query time: {elapsed} s")
#     # cache[sql_query] = result_dp
#
#     # never reveal the true result
#     # true_result = private_reader.reader.execute_df(sql_query)
#
#     return result_dp

