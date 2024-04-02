from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import duckdb
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn import tree
import pickle

@api_endpoint
def list_all_agents():
    return EscrowAPI.list_all_agents()

@api_endpoint
def list_all_des_with_src():
    return EscrowAPI.list_all_des_with_src()

@api_endpoint
def get_all_functions():
    return EscrowAPI.get_all_functions()

@api_endpoint
def get_function_info(function_name):
    return EscrowAPI.get_function_info(function_name)

@api_endpoint
def upload_data_in_csv(content):
    return EscrowAPI.CSVDEStore.write(content)

@api_endpoint
def propose_contract(dest_agents, des, f, *args, **kwargs):
    return EscrowAPI.propose_contract(dest_agents, des, f, *args, **kwargs)

@api_endpoint
def approve_contract(contract_id):
    return EscrowAPI.approve_contract(contract_id)

# What should the income query look like? We will do a query replacement
# select * from facebook
# select facebook.firstname from facebook

# update_query will do replace("facebook") with read_csv_auto("path_to_facebook") as facebook
# (and similarly for YouTube)
def update_query(query):
    from_index = query.lower().find("from")
    query_first_half = query[:from_index]
    query_second_half = query[from_index:]
    facebook_accessed = query.lower().find("facebook")
    youtube_accessed = query.lower().find("youtube")
    if facebook_accessed != -1:
        facebook_path = EscrowAPI.CSVDEStore.read(1)
        query_second_half = query_second_half.replace("facebook", f"read_csv_auto('{facebook_path}') as facebook", 1)
    if youtube_accessed != -1:
        youtube_path = EscrowAPI.CSVDEStore.read(2)
        query_second_half = query_second_half.replace("youtube", f"read_csv_auto('{youtube_path}') as youtube", 1)
    return query_first_half + query_second_half


@api_endpoint
@function
def run_query(query):
    """
    Run a user given query.
    """
    updated_query = update_query(query)
    conn = duckdb.connect()
    res_df = conn.execute(updated_query).fetchdf()
    conn.close()
    return res_df






