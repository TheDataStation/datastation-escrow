from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import duckdb
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn import tree
import pickle

@api_endpoint
def list_all_agents(user_id):
    return EscrowAPI.list_all_agents(user_id)

@api_endpoint
def list_all_des_with_src(user_id):
    return EscrowAPI.list_all_des_with_src(user_id)

@api_endpoint
def get_all_functions(user_id):
    return EscrowAPI.get_all_functions(user_id)

@api_endpoint
def get_function_info(user_id, function_name):
    return EscrowAPI.get_function_info(user_id, function_name)

@api_endpoint
def upload_advertising_data(user_id, content):
    return EscrowAPI.CSVDEStore.write(user_id, content)

# What should the income query look like? Let's still do the query replacement
# select * from facebook
# select facebook.firstname from facebook

# update_query will do replace("facebook") with read_csv_auto("path_to_facebook") as facebook


@function
def run_query_over_DE(query):
    """
    Run a user given query.
    """
    conn = duckdb.connect()
    res_df = conn.execute(updated_query).fetchdf()
    conn.close()
    return res_df






