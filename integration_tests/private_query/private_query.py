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

# from multiprocessing import Value

total_epsilon = 1.0
# global budget_spent
# budget_spent_global = Value("d", 0.0)

# record the data id for catalog (later we can change the db/storage structure so we don't have to record it here)
catalog_id = 0


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
def query_sql(context, sql_query, file_to_query, epsilon=None):
    files = utils.get_all_files()
    if file_to_query not in files:
        return f"{file_to_query} not accessible"

    if epsilon is None:
        epsilon = assign_budget(sql_query, total_epsilon)

    # if sql_query in cache.keys():
    #     return cache[sql_query]

    df = pd.read_csv(file_to_query)
    # df = df.convert_dtypes()

    # zz: generate the metadata required by smartnoise automatically
    name = pathlib.Path(file_to_query).stem
    metadata = get_metadata(df, name)
    # print("metadata:")
    # pprint.pprint(metadata)

    privacy = snsql.Privacy(epsilon=epsilon)
    private_reader = snsql.from_connection(df, metadata=metadata, privacy=privacy)

    eps_cost, delta_cost = private_reader.get_privacy_cost(sql_query)
    # eps_spent, delta_spent = private_reader.odometer.spent

    # TODO: record budget spent somewhere. can't store it as a variable inside the registered app
    #  since all functions are executed in isolated process and cannot change the state of the main process.
    #  (technically could store it in a database/file, but this is independent from data station)

    if "budget_spent" not in context:
        context["budget_spent"] = 0.0
    budget_spent = context["budget_spent"]

    if eps_cost + budget_spent > total_epsilon:
        print(f"error: budget exceeded. total budget = {total_epsilon}",
              f"budget spent = {budget_spent}",
              f"epsilon cost = {eps_cost}")
        return None

    context["budget_spent"] = budget_spent + eps_cost
    print(f"total budget = {total_epsilon}",
          f"budget spent = {budget_spent}",
          f"epsilon cost = {eps_cost}")

    start_time = time.time()
    result_dp = private_reader.execute_df(sql_query)
    elapsed = time.time() - start_time
    print(f"actual query time: {elapsed} s")
    # cache[sql_query] = result_dp

    # never reveal the true result
    # true_result = private_reader.reader.execute_df(sql_query)

    return result_dp


def get_metadata(df: pd.DataFrame, name: str):
    metadata = {}
    metadata[name] = {}  # collection
    metadata[name][name] = {}  # schema
    metadata[name][name][name] = {}  # table

    t = metadata[name][name][name]
    t["row_privacy"] = True
    t["rows"] = len(df)

    df_inferred = df.convert_dtypes()
    # print(df_inferred.dtypes)

    for col in df_inferred:
        t[str(col)] = {}
        c = t[str(col)]
        if pd.api.types.is_string_dtype(df_inferred[col]):
            c["type"] = "string"
        elif pd.api.types.is_bool_dtype(df_inferred[col]):
            c["type"] = "boolean"
        elif pd.api.types.is_datetime64_any_dtype(df_inferred[col]):
            c["type"] = "datetime"
        # elif pd.api.types.is_numeric_dtype(df_inferred[col]):
        #     if (df[col].fillna(-9999) % 1  == 0).all():
        #         c["type"] = "int"
        #     else:
        #         c["type"] = "float"
        elif pd.api.types.is_integer_dtype(df_inferred[col]) or pd.api.types.is_timedelta64_dtype(df_inferred[col]):
            c["type"] = "int"
            c["upper"] = str(df_inferred[col].max())
            c["lower"] = str(df_inferred[col].min())
        elif pd.api.types.is_float_dtype(df_inferred[col]):
            c["type"] = "float"
            c["upper"] = str(df_inferred[col].max())
            c["lower"] = str(df_inferred[col].min())
        else:
            raise ValueError("Unknown column type for column {0}".format(col))

    return metadata


def assign_budget(sql_query, total_epsilon):
    return total_epsilon / 10
