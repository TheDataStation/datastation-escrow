from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import duckdb
import numpy as np


@api_endpoint
def register_data(username,
                  data_name,
                  data_type,
                  access_param,
                  optimistic):
    print("This is a customized register data!")
    return EscrowAPI.register_data(username, data_name, data_type, access_param, optimistic)

@api_endpoint
def upload_data(username,
                data_id,
                data_in_bytes):
    return EscrowAPI.upload_data(username, data_id, data_in_bytes)

@api_endpoint
def suggest_share(username, agents, functions, data_elements):
    return EscrowAPI.suggest_share(username, agents, functions, data_elements)

@api_endpoint
def ack_data_in_share(username, data_id, share_id):
    return EscrowAPI.ack_data_in_share(username, data_id, share_id)

def get_data(de):
    if de.type == "file":
        return de.access_param

def get_column(de_id, attr_name):
    de = EscrowAPI.get_de_by_id(de_id)
    table_name = get_data(de)
    query = "SELECT " + attr_name + " From '" + table_name + "'"
    res = duckdb.sql(query).fetchall()
    return res

@api_endpoint
@function
def column_intersection(src_de_id, src_attr, tgt_de_id, tgt_attr):
    """
    Get column intersection between attributes within data elements
    """
    src_col = get_column(src_de_id, src_attr)
    tgt_col = get_column(tgt_de_id, tgt_attr)
    overlapping_values = set(src_col).intersection(set(tgt_col))
    count = len(overlapping_values)
    return count


@api_endpoint
@function
def compare_distributions(src_de_id, src_attr, tgt_de_id, tgt_attr):
    """
    Compare distributions between attributes within data elements
    """
    src_col = get_column(src_de_id, src_attr)
    tgt_col = get_column(tgt_de_id, tgt_attr)
    return np.mean(src_col), np.std(src_col), np.mean(tgt_col), np.std(tgt_col)
