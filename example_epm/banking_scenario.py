from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

@api_endpoint
def upload_transaction_data(user_id, de_in_bytes):
    return EscrowAPI.CSVDEStore.write(user_id, de_in_bytes)

@function
def show_schema():


