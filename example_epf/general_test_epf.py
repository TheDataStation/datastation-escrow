from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI
import csv

@api_endpoint
def register_de(username, data_name, data_type, access_param, optimistic):
    print("This is a customized register data!")
    return EscrowAPI.register_de(username, data_name, data_type, access_param, optimistic)

@api_endpoint
def upload_de(username, data_id, data_in_bytes):
    return EscrowAPI.upload_de(username, data_id, data_in_bytes)

@api_endpoint
def list_discoverable_des(username):
    return EscrowAPI.list_discoverable_des(username)

@api_endpoint
def suggest_share(username, dest_agents, data_elements, template, *args, **kwargs):
    return EscrowAPI.suggest_share(username, dest_agents, data_elements, template, *args, **kwargs)

@api_endpoint
def show_share(username, share_id):
    return EscrowAPI.show_share(username, share_id)

@api_endpoint
def approve_share(username, share_id):
    return EscrowAPI.approve_share(username, share_id)

@api_endpoint
def execute_share(username, share_id):
    return EscrowAPI.execute_share(username, share_id)

def get_data(de):
    if de.type == "file":
        return f"{de.access_param}"

@api_endpoint
@function
def print_first_row(de_id):
    de = EscrowAPI.get_de_by_id(de_id)
    file_path = get_data(de)
    with open(file_path, 'r') as csvfile:
        with open("output.csv", 'w', newline='') as outfile:
            reader = csv.reader(csvfile)
            writer = csv.writer(outfile)
            for row in reader:
                writer.writerow(row)
    return "write finished"
