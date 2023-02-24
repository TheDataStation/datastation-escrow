from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import csv


@api_endpoint
def register_dataset(username,
                     data_name,
                     data_in_bytes,
                     data_type,
                     optimistic,
                     original_data_size=None):
    print("This is a customized upload data!")
    EscrowAPI.register_dataset(username, data_name, data_in_bytes, data_type, optimistic, original_data_size)

@api_endpoint
def upload_policy(username, user_id, api, data_id):
    print("This is a customized upload policy!")
    EscrowAPI.upload_policy(username, user_id, api, data_id)

@api_endpoint
def suggest_share(username, agents, functions, data_elements):
    EscrowAPI.suggest_share(username, agents, functions, data_elements)

@api_endpoint
def ack_data_in_share(username, data_id, share_id):
    EscrowAPI.ack_data_in_share(username, data_id, share_id)

@function
def line_count():
    """count number of lines in a file"""
    print("starting counting line numbers")
    files = EscrowAPI.get_all_accessible_des()
    print(files)
    res = []
    for file in set(files):
        csv_file = open(file)
        reader = csv.reader(csv_file)
        res.append(len(list(reader)))
    return res

# create a PR for interceptor docker, and another one for this change
# restructuring the code
