from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI
from typing import List


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
def get_column(data_id, attr_name):
    return 0


# '''
# returns all data elements registered in enclave mode with the data station
# '''
# @api_endpoint
# @function
# def show_data_in_ds(username):
#     pass
#
# '''
# Returns description for DE provided during register
# '''
# @api_endpoint
# @function
# def show_de_description(username, data_id):
#     pass
#
# '''
# Returns format for DE
# '''
# @api_endpoint
# @function
# def show_de_format(username, data_id):
#     pass
#
# '''
# Returns schema for DE
# '''
# @api_endpoint
# @function
# def show_de_schema(username, data_id):
#     pass

# '''
# Performs private set intersection between values in column attr_name in DE
# data_id and either the contents of the `values` List or the contents of column
# my_attr_name in DE my_data_id
# '''
# @api_endpoint
# @function
# def column_intersection(data_id, attr_name: str, values: List = None,
#                         my_data_id=None, my_attr_name: str = None):
#     assert values is not None or (my_data_id is not None and my_attr_name is not None)
#     pass

# '''
# Compare distributions between attributes within data elements
# '''
# @api_endpoint
# @function
# def compare_distributions(username, data_id, attr_name, my_data_id, my_attr_name):
#     pass

# '''
# Suggest that the owner of `data_id` reformat the values in `attr_name` to match
# the format provided by `fmt`
# '''
# @api_endpoint
# @function
# def suggest_data_format(username, data_id, attr_name, fmt: str):
#     pass
#
# @api_endpoint
# @function
# def is_format_compatible(username, data_id, attr_name, my_data_id, my_attr_name):
#     pass
#
# '''
# Request random sample of column from data element; default sample size is 10 rows
# '''
# @api_endpoint
# @function
# def show_sample(username, data_id, attr_name, sample_size=10):
#     pass
