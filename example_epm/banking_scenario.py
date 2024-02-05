from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import csv
import pandas as pd
import duckdb
from scipy.stats import ks_2samp

@api_endpoint
def list_all_agents(user_id):
    return EscrowAPI.list_all_agents(user_id)

@api_endpoint
def register_de(user_id, de_name, discoverable):
    return EscrowAPI.register_de(user_id, de_name, "file", de_name, discoverable)

@api_endpoint
def upload_de(user_id, de_id, de_in_bytes):
    return EscrowAPI.upload_de(user_id, de_id, de_in_bytes)

@api_endpoint
def remove_de_from_storage(user_id, de_id):
    return EscrowAPI.remove_de_from_storage(user_id, de_id)

@api_endpoint
def remove_de_from_db(user_id, de_id):
    return EscrowAPI.remove_de_from_db(user_id, de_id)

@api_endpoint
def list_discoverable_des_with_src(user_id):
    return EscrowAPI.list_discoverable_des_with_src(user_id)

@api_endpoint
def get_all_functions(user_id):
    return EscrowAPI.get_all_functions(user_id)

@api_endpoint
def get_function_info(user_id, function_name):
    return EscrowAPI.get_function_info(user_id, function_name)

@api_endpoint
def propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs):
    return EscrowAPI.propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs)

@api_endpoint
def show_contract(user_id, contract_id):
    return EscrowAPI.show_contract(user_id, contract_id)

@api_endpoint
def show_all_contracts_as_dest(user_id):
    return EscrowAPI.show_all_contracts_as_dest(user_id)

@api_endpoint
def show_all_contracts_as_src(user_id):
    return EscrowAPI.show_all_contracts_as_src(user_id)

@api_endpoint
def approve_contract(user_id, contract_id):
    return EscrowAPI.approve_contract(user_id, contract_id)

@api_endpoint
def execute_contract(user_id, contract_id):
    return EscrowAPI.execute_contract(user_id, contract_id)

@function
def show_schema():
    """Return the schema of all DEs in contract, along with their IDs."""
    des = EscrowAPI.get_all_accessible_des()
    de_schema_dict = {}
    for de in des:
        with open(de.access_param, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)
            de_schema_dict[de.id] = header
    return de_schema_dict

@function
def check_column_compatibility(de_1, de_2, cols_1, cols_2):
    """
    Given two DEs, and two lists of column names from each DE, compare these columns 1 by 1.
    """
    # Loop through the cols array (needs to be same length)
    # For each pair: first get their types, then do KS test if both numerical, else jaccard similarity.
    de1 = EscrowAPI.get_de_by_id(de_1)
    de2 = EscrowAPI.get_de_by_id(de_2)
    conn = duckdb.connect()
    numerical_types = ['INT', 'BIGINT', 'SMALLINT', 'FLOAT', 'DOUBLE', 'DECIMAL']
    text_types = ['CHAR', 'VARCHAR', 'TEXT']
    date_time_types = ['DATE', 'TIME']
    boolean_types = ['BOOLEAN', 'BOOL']
    df_1 = conn.execute(f"SELECT * FROM read_csv_auto('{de1.access_param}')").fetchdf()
    df_2 = conn.execute(f"SELECT * FROM read_csv_auto('{de2.access_param}')").fetchdf()
    for i in range(len(cols_1)):
        col_1 = df_1[cols_1[i]]
        col_2 = df_2[cols_2[i]]
        type_1 = conn.execute(f"DESCRIBE SELECT {cols_1[i]} FROM read_csv_auto('{de1.access_param}')").fetchall()[0][1]
        type_2 = conn.execute(f"DESCRIBE SELECT {cols_2[i]} FROM read_csv_auto('{de2.access_param}')").fetchall()[0][1]
        print(cols_1[i])
        print(cols_2[i])
        # Case 1: both are numerical types: calculate their ks statistic
        if type_1 in numerical_types and type_2 in numerical_types:
            statistic, p_value = ks_2samp(col_1, col_2)
            if p_value > 0.05:
                print("Both numerical types. The distributions are likely similar.")
            else:
                print("Both numerical types. The distributions are likely different.")
        # Case 2: both are text types: run jaccard similarity
        elif type_1 in text_types and type_2 in text_types:
            intersection_size = len(set(col_1).intersection(set(col_2)))
            union_size = len(set(col_1).union(set(col_2)))
            jaccard_sim = intersection_size / union_size if union_size != 0 else 0
            print(f"Both string/text types. Jaccard similarity is {jaccard_sim}")
        elif type_1 in date_time_types and type_2 in date_time_types:
            print("Both date/time types.")
        else:
            print("Date types are different.")

@function
def share_sample(de_id, sample_size):
    de = EscrowAPI.get_de_by_id(de_id)
    df = pd.read_csv(de.access_param)
    sample_size = min(sample_size, len(df))
    sample = df.sample(n=sample_size)
    return sample

@function
def share_de(de_id):
    de = EscrowAPI.get_de_by_id(de_id)
    df = pd.read_csv(de.access_param)
    return df
