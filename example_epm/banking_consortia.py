from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

@api_endpoint
def list_all_agents(user_id):
    return EscrowAPI.list_all_agents(user_id)

@api_endpoint
def register_de(user_id, data_name, data_type, access_param, discoverable):
    return EscrowAPI.register_de(user_id, data_name, data_type, access_param, discoverable)

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

@api_endpoint
@function
def print_first_row(de_id):
    pass
