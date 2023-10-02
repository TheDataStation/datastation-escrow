from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI
import csv

@api_endpoint
def register_de(user_id, data_name, data_type, access_param, optimistic):
    print("This is a customized register data!")
    return EscrowAPI.register_de(user_id, data_name, data_type, access_param, optimistic)

@api_endpoint
def upload_de(user_id, data_id, data_in_bytes):
    return EscrowAPI.upload_de(user_id, data_id, data_in_bytes)

@api_endpoint
def list_discoverable_des(user_id):
    return EscrowAPI.list_discoverable_des(user_id)

@api_endpoint
def propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs):
    return EscrowAPI.propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs)

@api_endpoint
def show_contract(user_id, contract_id):
    return EscrowAPI.show_contract(user_id, contract_id)

@api_endpoint
def approve_contract(user_id, contract_id):
    return EscrowAPI.approve_contract(user_id, contract_id)

@api_endpoint
def execute_contract(user_id, contract_id):
    return EscrowAPI.execute_contract(user_id, contract_id)

def get_data(de):
    if de.type == "file":
        return f"{de.access_param}"

@api_endpoint
@function
def print_first_row(de_id):
    de = EscrowAPI.get_de_by_id(de_id)
    file_path = get_data(de)
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        first_row = next(reader)
    EscrowAPI.write_staged("temp", 1, first_row)
    return first_row
