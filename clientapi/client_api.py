import os
import pickle
import time
from flask import Flask, request
from multiprocessing import Process, Event, Queue, Manager

from dbservice import database_api

from common.pydantic_models.api import API
from common.pydantic_models.user import User
from common.pydantic_models.dataset import Dataset
from common.pydantic_models.response import Response
from common.pydantic_models.policy import Policy

from userregister import user_register
from dataregister import data_register
from policybroker import policy_broker
from gatekeeper import gatekeeper
from storagemanager.storage_manager import StorageManager
from stagingstorage.staging_storage import StagingStorage
from verifiability.log import Log
import pathlib
from writeaheadlog.write_ahead_log import WAL
from crypto.key_manager import KeyManager
from crypto import cryptoutils as cu
from dbservice.database import engine
from dsapplicationregistration.dsar_core import clear_register
from dbservice.database_api import clear_checkpoint_table_paths
from ds import DataStation

app = Flask(__name__)

def flask_thread(port, config):
    ds = DataStation(config, None)

    # TODO: make ds stateless

    # create user
    @app.route("/call_api")
    def call_api():
        unpickled = (request.get_data())
        print(unpickled)
        user_dict = pickle.loads(unpickled)
        print("User dictionary is", user_dict)

        return ds.create_user(user_dict['user'], user_dict['user_sym_key'], user_dict['user_pub_key'])

    # log in
    @app.route("/login_user")
    def login_user(username, password):
        response = user_register.login_user(username, password)
        if response.status == 0:
            return {"access_token": response.token, "token_type": "bearer"}
        else:
            # if password cannot correctly be verified, we return -1 to indicate login has failed
            return -1

    app.run(debug=False, host="localhost", port=port)
    return


class ClientAPI:

    """
    validates the login credentials of the user, then if the user is authorized
     the computation is passed to the ds class
    """
    def validate_and_get_username(token):
        # place for decryption of token
        # decrypt(token)

        # Perform authentication
        username = user_register.authenticate_user(token)

        return username

    def __init__(self,
                 config, port=8080):

        self.port = port

        self.server = Process(target=flask_thread, args=(self.port,config))
        return


    def start_server(self):
        self.server.start()



    def shut_down(self):


        print("client_api shut down complete")


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

    def upload_dataset(self,
                       data_name,
                       data_in_bytes,
                       data_type,
                       optimistic,
                       token,
                       original_data_size=None):

        self.validate_and_get_username(token)
        # parse HTTP request data (payload with the arguments)
        # self.ds.upload_dataset(parsed_data_name, parsed_data_in_bytes ...etc.)

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        # Decide which data_id to use from ClientAPI.cur_data_id field
        data_id = self.cur_data_id
        self.cur_data_id += 1

        # We first call SM to store the data
        # Note that SM needs to return access_type (how can the data element be accessed)
        # so that data_register can register this info

        storage_manager_response = self.storage_manager.store(data_name,
                                                              data_id,
                                                              data_in_bytes,
                                                              data_type,)
        if storage_manager_response.status == 1:
            return storage_manager_response

        # Storing data is successful. We now call data_register to register this data element in DB
        # Note: for file, access_type is the fullpath to the file
        access_type = storage_manager_response.access_type

        if self.trust_mode == "full_trust":
            data_register_response = data_register.register_data_in_DB(data_id,
                                                                       data_name,
                                                                       cur_username,
                                                                       data_type,
                                                                       access_type,
                                                                       optimistic)
        else:
            data_register_response = data_register.register_data_in_DB(data_id,
                                                                       data_name,
                                                                       cur_username,
                                                                       data_type,
                                                                       access_type,
                                                                       optimistic,
                                                                       self.write_ahead_log,
                                                                       self.key_manager,
                                                                       original_data_size)
        if data_register_response.status != 0:
            return Response(status=data_register_response.status,
                            message=data_register_response.message)

        return data_register_response

    # remote data element

    def remove_dataset(self, data_name, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        # First we call data_register to remove the existing dataset from the database
        if self.trust_mode == "full_trust":
            data_register_response = data_register.remove_data(data_name,
                                                               cur_username,)
        else:
            data_register_response = data_register.remove_data(data_name,
                                                               cur_username,
                                                               self.write_ahead_log,
                                                               self.key_manager,)
        if data_register_response.status != 0:
            return Response(status=data_register_response.status, message=data_register_response.message)

        # At this step we have removed the record about the dataset from DB
        # Now we remove its actual content from SM
        storage_manager_response = self.storage_manager.remove(data_name,
                                                               data_register_response.data_id,
                                                               data_register_response.type,)

        # If SM removal failed
        if storage_manager_response.status == 1:
            return storage_manager_response

        return Response(status=data_register_response.status, message=data_register_response.message)

    # create_policies

    def upload_policy(self, policy: Policy, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        if self.trust_mode == "full_trust":
            response = policy_broker.upload_policy(policy,
                                                   cur_username,)
        else:
            response = policy_broker.upload_policy(policy,
                                                   cur_username,
                                                   self.write_ahead_log,
                                                   self.key_manager,)

        return Response(status=response.status, message=response.message)

    # bulk upload policies
    # This is for testing purposes

    def bulk_upload_policies(self, policies, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        # This code here is unpolished. Just for testing purposes.
        if self.trust_mode == "full_trust":
            response = database_api.bulk_upload_policies(policies)
            return response
        else:
            response = policy_broker.bulk_upload_policies(policies,
                                                          cur_username,
                                                          self.write_ahead_log,
                                                          self.key_manager,)
            return response

    # delete_policies

    def remove_policy(self, policy: Policy, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        if self.trust_mode == "full_trust":
            response = policy_broker.remove_policy(policy,
                                                   cur_username,)
        else:
            response = policy_broker.remove_policy(policy,
                                                   cur_username,
                                                   self.write_ahead_log,
                                                   self.key_manager,)

        return Response(status=response.status, message=response.message)

    # list all available policies

    @staticmethod
    def get_all_policies():
        return policy_broker.get_all_policies()

    # data users actually calling the application apis

    def call_api(self, api: API, token, exec_mode, *args, **kwargs):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)
        # get caller's UID
        cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
        # If the user doesn't exist, something is wrong
        if cur_user.status == -1:
            print("Something wrong with the current user")
            return Response(status=1, message="Something wrong with the current user")
        cur_user_id = cur_user.data[0].id

        res = gatekeeper.call_api(api,
                                  cur_user_id,
                                  exec_mode,
                                  self.log,
                                  self.key_manager,
                                  self.accessible_data_dict,
                                  self.data_accessed_dict,
                                  *args,
                                  **kwargs)
        # Only when the returned status is 0 can we release the result
        if res.status == 0:
            api_result = res.result
            # We still need to encrypt the results using the caller's symmetric key if in no_trust_mode.
            if self.trust_mode == "no_trust":
                caller_symmetric_key = self.key_manager.get_agent_symmetric_key(cur_user_id)
                api_result = cu.encrypt_data_with_symmetric_key(cu.to_bytes(api_result), caller_symmetric_key)
            return api_result
        # In this case we need to put result into staging storage, so that they can be released later
        elif res.status == -1:
            api_result = res.result[0]
            data_ids_accessed = res.result[1]
            # We first convert api_result to bytes because we need to store it in staging storage
            # In full_trust mode, we convert it to bytes directly
            if self.trust_mode == "full_trust":
                api_result = cu.to_bytes(api_result)
            # In no_trust mode, we encrypt it using caller's symmetric key
            else:
                caller_symmetric_key = self.key_manager.get_agent_symmetric_key(cur_user_id)
                api_result = cu.encrypt_data_with_symmetric_key(cu.to_bytes(api_result), caller_symmetric_key)

            # print(api_result)
            # print(data_ids_accessed)

            # Call staging storage to store the bytes

            # Decide which data_id to use from ClientAPI.cur_data_id field
            staging_data_id = self.cur_staging_data_id
            self.cur_staging_data_id += 1

            staging_storage_response = self.staging_storage.store(staging_data_id,
                                                                  api_result)
            if staging_storage_response.status == 1:
                return staging_storage_response

            # Storing into staging storage is successful. We now call data_register to register this staging DE in DB.
            # We need to store to both the staged table and the provenance table.

            # Full_trust mode
            if self.trust_mode == "full_trust":
                # Staged table
                data_register_response_staged = data_register.register_staged_in_DB(staging_data_id,
                                                                                    cur_user_id,
                                                                                    api,)
                # Provenance table
                data_register_response_provenance = data_register.register_provenance_in_DB(staging_data_id,
                                                                                            data_ids_accessed,)
            else:
                # Staged table
                data_register_response_staged = data_register.register_staged_in_DB(staging_data_id,
                                                                                    cur_user_id,
                                                                                    api,
                                                                                    self.write_ahead_log,
                                                                                    self.key_manager,
                                                                                    )
                # Provenance table
                data_register_response_provenance = data_register.register_provenance_in_DB(staging_data_id,
                                                                                            data_ids_accessed,
                                                                                            cur_user_id,
                                                                                            self.write_ahead_log,
                                                                                            self.key_manager,
                                                                                            )
            if data_register_response_staged.status != 0 or data_register_response_provenance.status != 0:
                return Response(status=data_register_response_staged.status,
                                message="internal database error")
            res_msg = "Staged data ID " + str(staging_data_id)
            return res_msg
        else:
            return res.message

    # data users gives a staged DE ID and tries to release it
    def release_staged_DE(self, staged_ID, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)
        # get caller's UID
        cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
        # If the user doesn't exist, something is wrong
        if cur_user.status == -1:
            print("Something wrong with the current user")
            return Response(status=1, message="Something wrong with the current user")
        cur_user_id = cur_user.data[0].id

        # First get the API call that generated this staged DE
        api = database_api.get_api_for_staged_id(staged_ID)

        # Then get the currently accessible DEs for the <cur_user_id, api> combo
        accessible_data_ids = policy_broker.get_user_api_info(cur_user_id, api)

        # Then get the parent_ids for this staged DE (all accessed DEs used to create this DE)
        accessed_data_ids = database_api.get_parent_id_for_staged_id(staged_ID)

        # Then check if accessed_data_ids is a subset of accessible_data_ids
        # If yes, we can release the staged DE
        if set(accessed_data_ids).issubset(set(accessible_data_ids)):
            res = cu.from_bytes(self.staging_storage.release(staged_ID))
            return res

    # print out the contents of the log

    def read_full_log(self):
        self.log.read_full_log(self.key_manager)

    # print out the contents of the WAL

    def read_wal(self):
        self.write_ahead_log.read_wal(self.key_manager)

    # recover DB from the contents of the WAL

    def recover_db_from_wal(self):
        # Step 1: restruct the DB
        self.write_ahead_log.recover_db_from_wal(self.key_manager)

        # Step 2: reset self.cur_user_id from DB
        user_id_resp = database_api.get_user_with_max_id()
        if user_id_resp.status == 1:
            self.cur_user_id = user_id_resp.data[0].id + 1
        else:
            self.cur_user_id = 1
        print("User ID to use after recovering DB is: "+str(self.cur_user_id))

        # Step 3: reset self.cur_data_id from DB
        data_id_resp = database_api.get_data_with_max_id()
        if data_id_resp.status == 1:
            self.cur_data_id = data_id_resp.data[0].id + 1
        else:
            self.cur_data_id = 1
        print("Data ID to use after recovering DB is: " + str(self.cur_data_id))

    # For testing purposes: persist keys to a file

    def save_symmetric_keys(self):
        with open("symmetric_keys.pkl", 'ab') as keys:
            agents_symmetric_key = pickle.dumps(self.key_manager.agents_symmetric_key)
            keys.write(agents_symmetric_key)

    # For testing purposes: read keys from a file

    def load_symmetric_keys(self):
        with open("symmetric_keys.pkl", "rb") as keys:
            agents_symmetric_key = pickle.load(keys)
            self.key_manager.agents_symmetric_key = agents_symmetric_key


if __name__ == "__main__":
    print("Client API")
