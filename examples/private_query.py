import glob
import os
import pathlib
from io import BytesIO

import yaml

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
    # TODO: for now just get the schema from existing aggregated data (only one file in storage)

    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = set(glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True))

    for file in files:
        # print(file)
        if pathlib.Path(file).is_file() and pathlib.Path(file).suffix == ".yaml":
            with open(file, "rb") as cur_file:
                content = cur_file.read().rstrip(b'\x00')
                catalog = yaml.load(BytesIO(content))
                return catalog

@register()
def query_sql(sql_query, epsilon=None):
    if epsilon is None:
        epsilon = assign_budget(sql_query, total_epsilon)

    if sql_query in cache.keys():
        return cache[sql_query]

    # assume there is one aggregated dataset to query
    csv_path = 'mock'
    meta_path = 'mock'

    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = set(glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True))

    for file in files:
        # print(file)
        if pathlib.Path(file).is_file():
            if pathlib.Path(file).suffix == ".csv":
                csv_path = pathlib.Path(file).absolute()
            if pathlib.Path(file).suffix == ".yaml":
                meta_path = pathlib.Path(file).absolute()

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


def assign_budget(sql_query, total_epsilon):
    return total_epsilon/10
