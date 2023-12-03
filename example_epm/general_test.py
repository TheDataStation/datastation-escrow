from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI
import csv

@api_endpoint
def list_all_agents(user_id):
    return EscrowAPI.list_all_agents(user_id)

@api_endpoint
def register_de(user_id, data_name, data_type, access_param, discoverable):
    print("This is a customized register DE!")
    return EscrowAPI.register_de(user_id, data_name, data_type, access_param, discoverable)

@api_endpoint
def upload_de(user_id, de_id, data_in_bytes):
    return EscrowAPI.upload_de(user_id, de_id, data_in_bytes)

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
def propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs):
    return EscrowAPI.propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs)

@api_endpoint
def show_contract(user_id, contract_id):
    return EscrowAPI.show_contract(user_id, contract_id)

@api_endpoint
def show_all_contracts_for_dest(user_id):
    return EscrowAPI.show_all_contracts_for_dest(user_id)

@api_endpoint
def show_all_contracts_for_src(user_id):
    return EscrowAPI.show_all_contracts_for_src(user_id)

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
        print(first_row)
    # EscrowAPI.write_staged("temp", 1, first_row)
    return first_row
