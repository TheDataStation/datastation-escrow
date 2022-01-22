from dbservice import database_api

from models.api import *
from models.user import *
from models.response import *

import user_register
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

def get_all_apis(token):

    # Perform authentication
    user_register.authenticate_user(token)

    # Call policy_broker directly
    return policy_broker.get_all_apis()

# list_application_apis

# User Agent APIs

# call(function_name: str, args: dict):

# Owner Agent APIs

# upload data element

# remote data element

# list_policies, list_policies(data_element), list_policies(fn)

# create_policies

# delete_policies

# Operator Agent APIs

# Developer APIs (may be removed in future)

# upload a new API

def upload_api(api: API):
    response = database_api.create_api(api)
    return Response(status=response.status, message=response.msg)


if __name__ == "__main__":
    print("Client API")