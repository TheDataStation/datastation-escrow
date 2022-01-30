import random

from dbservice import database_api

from models.api import *
from models.api_dependency import *
from models.user import *
from models.response import *
from models.policy import *

from userregister import user_register
from dataregister import data_register
from policybroker import policy_broker
from gatekeeper import gatekeeper
from storagemanager.storage_manager import StorageManager


class ClientAPI:

    def __init__(self, storageManager: StorageManager):
        self.storage_manager = storageManager

    # create user

    @staticmethod
    def create_user(user: User):
        response = user_register.create_user(user)
        return Response(status=response.status, message=response.message)

    # log in

    @staticmethod
    def login_user(username, password):
        response = user_register.login_user(username, password)
        if response.status == 0:
            return {"access_token": response.token, "token_type": "bearer"}
        else:
            # if password cannot correctly be verified, we return -1 to indicate login has failed
            return -1

    # list application apis

    @staticmethod
    def get_all_apis(token):

        # Perform authentication
        user_register.authenticate_user(token)

        # Call policy_broker directly
        return policy_broker.get_all_apis()

    # list application api dependencies

    @staticmethod
    def get_all_api_dependencies(token):

        # Perform authentication
        user_register.authenticate_user(token)

        # Call policy_broker directly
        return policy_broker.get_all_dependencies()

    # upload data element

    def upload_dataset(self, data_name, data_in_bytes, data_type, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        # TODO: call identity_manager to compute the dataset id

        data_id = random.randint(1, 100000)

        # First we call data_register to register a new dataset in the database
        data_register_response = data_register.upload_data(data_id,
                                                           data_name,
                                                           cur_username,
                                                           data_type)
        if data_register_response.status != 0:
            return Response(status=data_register_response.status, message=data_register_response.message)

        # Registration of the dataset is successful. Now we call SM
        storage_manager_response = self.storage_manager.store(data_name,
                                                              data_id,
                                                              data_in_bytes)
        # If dataset already exists
        if storage_manager_response.status == 1:
            return storage_manager_response

        return data_register_response

    # remote data element

    def remove_dataset(self, data_name, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        # First we call data_register to remove the existing dataset from the database
        data_register_response = data_register.remove_data(data_name, cur_username)
        if data_register_response.status != 0:
            return Response(status=data_register_response.status, message=data_register_response.message)

        # At this step we have removed the record about the dataset from DB
        # Now we remove its actual content from SM
        storage_manager_response = self.storage_manager.remove(data_name,
                                                               data_register_response.data_id)

        # If SM removal failed
        if storage_manager_response.status == 1:
            return storage_manager_response

        return Response(status=data_register_response.status, message=data_register_response.message)

    # create_policies

    @staticmethod
    def upload_policy(policy: Policy, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        response = policy_broker.upload_policy(policy, cur_username)
        return Response(status=response.status, message=response.message)

    # delete_policies

    @staticmethod
    def remove_policy(policy: Policy, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        response = policy_broker.remove_policy(policy, cur_username)
        return Response(status=response.status, message=response.message)

    # list all available policies

    @staticmethod
    def get_all_policies():
        return policy_broker.get_all_policies()

    # # upload a new API
    #
    # @staticmethod
    # def upload_api(api: API):
    #     response = database_api.create_api(api)
    #     return Response(status=response.status, message=response.msg)
    #
    # # Upload a new API Dependency
    #
    # @staticmethod
    # def upload_api_dependency(api_dependency: APIDependency):
    #     response = database_api.create_api_dependency(api_dependency)
    #     return Response(status=response.status, message=response.msg)

    # data users actually calling the application apis

    @staticmethod
    def call_api(api: API, *args, **kwargs):
        res = gatekeeper.call_api(api, *args, **kwargs)
        return res

    # TODO: remove this in the future once interceptor is integrated
    @staticmethod
    def get_accessible_data(user_id: int, api: str):
        gatekeeper.get_accessible_data(user_id, api)


if __name__ == "__main__":
    print("Client API")
