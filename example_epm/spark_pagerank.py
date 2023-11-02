from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import re
import sys
from operator import add

from pyspark.sql import SparkSession

@api_endpoint
def register_de(user_id: int,
                file_name: str, ):
    """Register a DE"""
    return EscrowAPI.register_de(user_id, file_name, "file", file_name, 1)


@api_endpoint
def upload_de(user_id, data_id, data_in_bytes):
    return EscrowAPI.upload_de(user_id, data_id, data_in_bytes)


@api_endpoint
def list_discoverable_des(user_id: int):
    return EscrowAPI.list_discoverable_des(user_id)


@api_endpoint
def propose_contract(user_id: int,
                     dest_agents: list[int],
                     data_elements: list[int],
                     f: str, ):
    return EscrowAPI.propose_contract(user_id, dest_agents, data_elements, f)


@api_endpoint
def show_contract(user_id: int, contract_id: int):
    return EscrowAPI.show_contract(user_id, contract_id)


@api_endpoint
def approve_contract(user_id: int, contract_id: int):
    return EscrowAPI.approve_contract(user_id, contract_id)


@api_endpoint
def execute_contract(user_id: int, contract_id: int):
    return EscrowAPI.execute_contract(user_id, contract_id)