import pickle

from dbservice import database_api

from models.api import *
from models.api_dependency import *
from models.user import *
from models.dataset import *
from models.response import *
from models.policy import *

from userregister import user_register
from dataregister import data_register
from policybroker import policy_broker
from gatekeeper import gatekeeper
from storagemanager.storage_manager import StorageManager
from verifiability.log import Log
from writeaheadlog.write_ahead_log import WAL
from crypto.key_manager import KeyManager
from crypto import cryptoutils as cu

class ClientAPI:

    def __init__(self,
                 storageManager: StorageManager,
                 data_station_log: Log,
                 write_ahead_log: WAL,
                 keyManager: KeyManager,
                 trust_mode: str):
        self.storage_manager = storageManager
        self.log = data_station_log
        self.write_ahead_log = write_ahead_log
        self.key_manager = keyManager

        # The following field decides the trust mode for the DS
        self.trust_mode = trust_mode

        # The following field decides which user_id we should use when we upload a new user
        # Right now we are just incrementing by 1
        user_id_resp = database_api.get_user_with_max_id()
        if user_id_resp.status == 1:
            self.cur_user_id = user_id_resp.data[0].id + 1
        else:
            self.cur_user_id = 1
        # print("Starting user id should be:")
        # print(self.cur_user_id)

        # The following field decides which data_id we should use when we upload a new data
        # Right now we are just incrementing by 1
        data_id_resp = database_api.get_data_with_max_id()
        if data_id_resp.status == 1:
            self.cur_data_id = data_id_resp.data[0].id + 1
        else:
            self.cur_data_id = 1
        # print("Starting data id should be:")
        # print(self.cur_data_id)

    # create user

    def create_user(self, user: User, user_sym_key=None, user_public_key=None):

        # First we decide which user_id to use from ClientAPI.cur_user_id field
        user_id = self.cur_user_id
        self.cur_user_id += 1

        # Part one: register this user's symmetric key and public key
        if self.trust_mode == "no_trust":
            self.key_manager.store_agent_symmetric_key(user_id, user_sym_key)
            self.key_manager.store_agent_public_key(user_id, user_public_key)

        # Part two: Call the user_register to register the user in the DB
        if self.trust_mode == "full_trust":
            response = user_register.create_user(user_id,
                                                 user.user_name,
                                                 user.password,)
        else:
            response = user_register.create_user(user_id,
                                                 user.user_name,
                                                 user.password,
                                                 self.write_ahead_log,
                                                 self.key_manager,)

        if response.status == 1:
            return Response(status=response.status, message=response.message)

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

    def upload_dataset(self,
                       data_name,
                       data_in_bytes,
                       data_type,
                       optimistic,
                       token):

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
        access_type = storage_manager_response.access_type

        if self.trust_mode == "full_trust":
            data_register_response = data_register.upload_data(data_id,
                                                               data_name,
                                                               cur_username,
                                                               data_type,
                                                               access_type,
                                                               optimistic,)
        else:
            data_register_response = data_register.upload_data(data_id,
                                                               data_name,
                                                               cur_username,
                                                               data_type,
                                                               access_type,
                                                               optimistic,
                                                               self.write_ahead_log,
                                                               self.key_manager,)
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

    def call_api(self, api: API, token, exec_mode, *args, **kwargs):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        res = gatekeeper.call_api(api,
                                  cur_username,
                                  exec_mode,
                                  self.log,
                                  self.key_manager,
                                  *args,
                                  **kwargs,)
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

    # For testing purposes: retrieve a file from the storage

    def retrieve_data_by_id(self, data_id, token):

        # Perform authentication
        cur_username = user_register.authenticate_user(token)

        # First get the data element's info from DB
        resp = database_api.get_dataset_by_id(data_id)
        if resp.status != 1:
            return resp

        # If there is no error, we call store_manager.retrieve_data_by_id

        storage_manager_response = self.storage_manager.retrieve_data_by_id(resp.data[0].type,
                                                                            resp.data[0].access_type,)
        if storage_manager_response.status == 1:
            return storage_manager_response

        data_to_return = storage_manager_response.data

        # There are two cases here
        # 1) full trust mode: the data is not encrypted, we can return it directly
        # 2) no trust mode: we need to decrypt the data, re-encrypt it using caller's symmetric key, then return
        if self.trust_mode == "no_trust":
            # First get caller's id
            cur_user = database_api.get_user_by_user_name(User(user_name=cur_username,))
            # If the user doesn't exist, something is wrong
            if cur_user.status == -1:
                print("Something wrong with the current user")
                return Response(status=1, message="Something wrong with the current user")
            cur_user_id = cur_user.data[0].id

            # Then get data element's owner id
            data_owner_response = database_api.get_dataset_owner(Dataset(id=data_id,))
            if data_owner_response.status == -1:
                return Response(status=1, message="Error retrieving data owner.")
            data_owner_id = data_owner_response.data[0].id

            # We get the owner's symmetric key and decrypt the file
            old_sym_key = self.key_manager.agents_symmetric_key[data_owner_id]
            plain_data = cu.decrypt_data_with_symmetric_key(data_to_return, old_sym_key)

            # We get the caller's symmetric key and encrypt the file
            new_sym_key = self.key_manager.agents_symmetric_key[cur_user_id]
            data_to_return = cu.encrypt_data_with_symmetric_key(plain_data, new_sym_key)

        return data_to_return


if __name__ == "__main__":
    print("Client API")
