from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

@api_endpoint
def register_de(username, data_name, data_type, access_param, optimistic):
    print("This is a customized register data!")
    return EscrowAPI.register_de(username, data_name, data_type, access_param, optimistic)

@api_endpoint
def upload_de(username, data_id, data_in_bytes):
    return EscrowAPI.upload_de(username, data_id, data_in_bytes)

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