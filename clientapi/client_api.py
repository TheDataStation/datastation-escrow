from dbservice import database_api

from models.api import *
from models.api_dependency import *
from models.user import *
from models.response import *
from models.policy import *

import user_register
import data_register
import policy_broker

# Agent APIs

# create user

def create_user(user: User):
    response = user_register.create_user(user)
    return Response(status=response.status, message=response.message)

# login

def login_user(username, password):
    response = user_register.login_user(username, password)
    if response.status == 0:
        return {"access_token": response.token, "token_type": "bearer"}
    else:
        # if password cannot correctly be verified, we return -1 to indicate login has failed
        return -1

# list application apis

def get_all_apis(token):

    # Perform authentication
    user_register.authenticate_user(token)

    # Call policy_broker directly
    return policy_broker.get_all_apis()

# list application api dependencies

def get_all_api_dependencies(token):

    # Perform authentication
    user_register.authenticate_user(token)

    # Call policy_broker directly
    return policy_broker.get_all_dependencies()

# User Agent APIs

# call(function_name: str, args: dict):

# Owner Agent APIs

# upload data element

def upload_dataset(data_name, data_in_bytes, token):

    # Perform authentication
    cur_username = user_register.authenticate_user(token)

    response = data_register.upload_data(data_name, data_in_bytes, cur_username)
    if response.status != 0:
        return Response(status=response.status, message=response.message)

    return response

# remote data element

def remove_dataset(data_name, token):

    # Perform authentication
    cur_username = user_register.authenticate_user(token)

    response = data_register.remove_data(data_name, cur_username)
    return Response(status=response.status, message=response.message)

# list_policies, list_policies(data_element), list_policies(fn)

# create_policies

def upload_policy(policy: Policy, token):

    # Perform authentication
    cur_username = user_register.authenticate_user(token)

    response = policy_broker.upload_policy(policy, cur_username)
    return Response(status=response.status, message=response.message)

# delete_policies

def remove_policy(policy: Policy, token):

    # Perform authentication
    cur_username = user_register.authenticate_user(token)

    response = policy_broker.remove_policy(policy, cur_username)
    return Response(status=response.status, message=response.message)

# Operator Agent APIs

# list all available policies

def get_all_policies():
    return policy_broker.get_all_policies()

# Developer APIs (may be removed in future)

# upload a new API

def upload_api(api: API):
    response = database_api.create_api(api)
    return Response(status=response.status, message=response.msg)

# Upload a new API Dependency

def upload_api_dependency(api_dependency: APIDependency):
    response = database_api.create_api_dependency(api_dependency)
    return Response(status=response.status, message=response.msg)


if __name__ == "__main__":
    print("Client API")