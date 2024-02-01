from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

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
    pass
