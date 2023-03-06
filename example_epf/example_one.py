from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import csv
import psycopg2
import requests


def get_data(de):
    if de.type == "file":
        return de.access_param
    elif de.type == "postgres":
        conn_string = de.access_param["credentials"]
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        query = de.access_param["query"]
        cursor.execute(query)
        res = []
        for row in cursor:
            res.append(row)
        return res
    elif de.type == "opendata":
        api_key = de.access_param["api_key"]
        endpoint = de.access_param["endpoint"]
        headers = {'X-App-Token': api_key}
        result = requests.get(endpoint, headers=headers)
        return result.json()[0:100]
    else:
        return 0


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

@api_endpoint
@function
def retrieve_data():
    """get the content of the data elements"""
    accessible_de = EscrowAPI.get_all_accessible_des()
    res = []
    for cur_de in set(accessible_de):
        print(cur_de.type)
        if cur_de.type == "file":
            file_name = get_data(cur_de)
            csv_file = open(file_name)
            reader = csv.reader(csv_file)
            res.append(list(reader))
        else:
            data = get_data(cur_de)
            res.append(data)
    return res

@api_endpoint
@function
def line_count():
    """count number of lines in a file"""
    print("starting counting line numbers")
    accessible_de = EscrowAPI.get_all_accessible_des()
    res = []
    for cur_de in set(accessible_de):
        file_name = get_data(cur_de)
        csv_file = open(file_name)
        reader = csv.reader(csv_file)
        res.append(len(list(reader)))
    return res

