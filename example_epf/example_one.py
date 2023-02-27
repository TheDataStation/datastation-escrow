from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import csv


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
def upload_policy(username, user_id, api, data_id):
    print("This is a customized upload policy!")
    return EscrowAPI.upload_policy(username, user_id, api, data_id)

@api_endpoint
def suggest_share(username, agents, functions, data_elements):
    return EscrowAPI.suggest_share(username, agents, functions, data_elements)

@api_endpoint
def ack_data_in_share(username, data_id, share_id):
    return EscrowAPI.ack_data_in_share(username, data_id, share_id)

@function
def get_data():
    """get the content of the data elements"""
    des = EscrowAPI.get_all_accessible_des()

@function
def line_count():
    """count number of lines in a file"""
    print("starting counting line numbers")
    accessible_de = EscrowAPI.get_all_accessible_des()
    res = []
    for cur_de in set(accessible_de):
        file = cur_de.get_data()
        csv_file = open(file)
        reader = csv.reader(csv_file)
        res.append(len(list(reader)))
    return res
