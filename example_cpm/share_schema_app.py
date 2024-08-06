from dsapplicationregistration.dsar_core import api_endpoint, function
from contractapi.contract_api import ContractAPI

import csv


@api_endpoint
def upload_data_in_csv(data_in_bytes):
    return ContractAPI.CSVDEStore.write(data_in_bytes)


@api_endpoint
def propose_contract(dest_agents, des, f, *args, **kwargs):
    return ContractAPI.propose_contract(dest_agents, des, f, *args, **kwargs)


@api_endpoint
def approve_contract(contract_id):
    return ContractAPI.approve_contract(contract_id)

@api_endpoint
@function
def show_schema(de_ids):
    """Return the schema of all DEs in contract, along with their IDs."""
    de_schema_dict = {}
    # Following line tests short-circuiting
    for de_id in de_ids:
        de_path = ContractAPI.CSVDEStore.read(de_id)
        with open(de_path, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)
            de_schema_dict[de_id] = header
    return de_schema_dict
