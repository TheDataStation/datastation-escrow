from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi import escrow_api

import csv


@api_endpoint
def register_data(ds,
                  username,
                  data_name,
                  data_in_bytes,
                  data_type,
                  optimistic):
    # print("This is a customized upload data!")
    res = escrow_api.register_data(ds, username, data_name, data_in_bytes, data_type, optimistic)
    return res

@api_endpoint
def upload_data(ds,
                username,
                data_id,
                data_in_bytes):
    res = escrow_api.upload_data(ds, username, data_id, data_in_bytes)
    return res

@api_endpoint
def upload_policy(ds, username, user_id, api, data_id):
    print("This is a customized upload policy!")
    escrow_api.upload_policy(ds, username, user_id, api, data_id)

@api_endpoint
def suggest_share(ds, username, agents, functions, data_elements):
    escrow_api.suggest_share(ds, username, agents, functions, data_elements)

@api_endpoint
def ack_data_in_share(ds, username, data_id, share_id):
    escrow_api.ack_data_in_share(ds, username, data_id, share_id)

@function
def line_count():
    """count number of lines in a file"""
    print("starting counting line numbers")
    files = escrow_api.get_all_files()
    print(files)
    res = []
    for file in set(files):
        csv_file = open(file)
        reader = csv.reader(csv_file)
        res.append(len(list(reader)))
    return res
