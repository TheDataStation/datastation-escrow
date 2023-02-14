import os
import sys
import multiprocessing
import pathlib
import time
import pickle

from common.pydantic_models.api import API
from common.pydantic_models.user import User
from common.pydantic_models.response import Response
from common.pydantic_models.policy import Policy

from common.general_utils import parse_config

from dbservice.database_api import set_checkpoint_table_paths, recover_db_from_snapshots
from storagemanager.storage_manager import StorageManager
from stagingstorage.staging_storage import StagingStorage
from policybroker import policy_broker
from dataregister import data_register
from sharemanager import share_manager
from verifiability.log import Log
from writeaheadlog.write_ahead_log import WAL
from crypto import cryptoutils as cu
from crypto.key_manager import KeyManager
from gatekeeper.gatekeeper import Gatekeeper
from dbservice import database_api
from dbservice.database import engine
from dbservice.database_api import clear_checkpoint_table_paths
from dsapplicationregistration.dsar_core import (get_registered_api_endpoint,
                                                 clear_api_endpoint,
                                                 clear_function, )
from userregister import user_register


class DataStation:

    class DSConfig:
        def __init__(self, ds_config):
            """
            Class that stores DS config variables at the time they were given
            """
            # get the trust mode for the data station
            self.trust_mode = ds_config["trust_mode"]

            # get storage path for data
            self.storage_path = ds_config["storage_path"]

            # staging path
            self.staging_path = ds_config["staging_path"]

            # log arguments
            self.log_in_memory_flag = ds_config["log_in_memory"]
            self.log_path = ds_config["log_path"]

            # wal arguments
            self.wal_path = ds_config["wal_path"]
            self.check_point_freq = ds_config["check_point_freq"]

            # the table_paths in dbservice.check_point
            self.table_paths = ds_config["table_paths"]

            # interceptor paths
            self.ds_storage_path = str(pathlib.Path(
                ds_config["storage_path"]).absolute())
            self.mount_point = str(pathlib.Path(
                ds_config["mount_path"]).absolute())

    def __init__(self, ds_config, app_config, need_to_recover=False):
        """
        The general class that creates and initializes a Data Station.

        Parameters:
         ds_config: dictionary of general config options
         app_config: dictionary of app connector options TODO: store this as variables
         need_to_recover: bool that specifies if needs to recover from previous database
        """
        # parse config file
        self.config = self.DSConfig(ds_config)

        # set up trust mode
        self.trust_mode = self.config.trust_mode

        # set up an instance of the storage_manager
        storage_path = self.config.storage_path
        self.storage_manager = StorageManager(storage_path)

        # set up an instance of the staging_storage
        staging_path = self.config.staging_path
        self.staging_storage = StagingStorage(staging_path)

        # set up an instance of the log
        log_in_memory_flag = self.config.log_in_memory_flag
        log_path = self.config.log_path
        self.data_station_log = Log(
            log_in_memory_flag, log_path, self.trust_mode)

        # set up an instance of the write ahead log
        wal_path = self.config.wal_path
        check_point_freq = self.config.check_point_freq
        self.write_ahead_log = WAL(wal_path, check_point_freq)

        # set up mount point
        mount_point = self.config.mount_point
        self.accessible_data_dict = {}
        self.data_accessed_dict = {}

        # set up an instance of the key manager
        self.key_manager = KeyManager()

        print(mount_point)
        print(storage_path)
        # set up the gatekeeper
        self.epf_path = app_config["epf_path"]
        self.gatekeeper = Gatekeeper(
            self.data_station_log,
            self.write_ahead_log,
            self.key_manager,
            self.trust_mode,
            self.accessible_data_dict,
            self.data_accessed_dict,
            self.epf_path,
            self.config.ds_storage_path
        )

        # set up the table_paths in dbservice.check_point
        table_paths = self.config.table_paths
        set_checkpoint_table_paths(table_paths)

        # Lastly, if we are in recover mode, we need to call
        if need_to_recover:
            self.load_symmetric_keys()
            recover_db_from_snapshots(self.key_manager)
            self.recover_db_from_wal()

        # The following field decides which data_id we should use when we upload a new DE
        # Right now we are just incrementing by 1
        data_id_resp = database_api.get_data_with_max_id()
        if data_id_resp.status == 1:
            self.cur_data_id = data_id_resp.data[0].id + 1
        else:
            self.cur_data_id = 1
        # print("Starting data id should be:")
        # print(self.cur_data_id)

        # The following fields decides which staging_data_id we should use at a new insertion
        staging_id_resp = database_api.get_staging_with_max_id()
        if staging_id_resp.status == 1:
            self.cur_staging_data_id = staging_id_resp.data[0].id + 1
        else:
            self.cur_staging_data_id = 1

        # The following field decides which user_id we should use
        user_id_resp = database_api.get_user_with_max_id()
        if user_id_resp.status == 1:
            self.cur_user_id = user_id_resp.data[0].id + 1
        else:
            self.cur_user_id = 1

        # The following field decides which share_id we should use
        share_id_resp = database_api.get_share_with_max_id()
        if share_id_resp.status == 1:
            self.cur_share_id = share_id_resp.data[0].id + 1
        else:
            self.cur_share_id = 1

    def create_user(self, user: User, user_sym_key=None, user_public_key=None):
        """
        Creates a user in DS

        Parameters:
         user: user model
         user_sym_key: for no trust
         user_public_key: for no trust

        Returns:
         Response on user creation
        """

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

    @staticmethod
    def get_all_apis():
        """
        Gets all APIs from the policy broker

        Parameters:
         Nothing

        Returns:
         all apis
        """
        # Call policy_broker directly
        return policy_broker.get_all_apis()

    @staticmethod
    def get_all_api_dependencies():
        """
        Gets all API dependencies from the policy broker

        Parameters:
         Nothing

        Returns:
         all dependencies
        """

        # Call policy_broker directly
        return policy_broker.get_all_dependencies()

    def register_dataset(self,
                         username,
                         data_name,
                         data_in_bytes,
                         data_type,
                         optimistic,
                         original_data_size=None):
        """
        Uploads a dataset to DS, tied to a specific user

        Parameters:
         username: the unique username identifying which user owns the dataset
         data_name: name of the data
         data_in_bytes: size of data to be uploaded
         data_type: TODO: what types of data are able to be uploaded?
         optimistic: flag to be included in optimistic data discovery

        Returns:
         Response of data register
        """
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
                                                                       username,
                                                                       data_type,
                                                                       access_type,
                                                                       optimistic)
        else:
            data_register_response = data_register.register_data_in_DB(data_id,
                                                                       data_name,
                                                                       username,
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

    def remove_dataset(self, username, data_name):
        """
        Removes a dataset from DS

        Parameters:
         username: the unique username identifying which user owns the dataset
         data_name: name of the data

        Returns:
         Response of data register
        """

        # First we call data_register to remove the existing dataset from the database
        if self.trust_mode == "full_trust":
            data_register_response = data_register.remove_data(data_name,
                                                               username,)
        else:
            data_register_response = data_register.remove_data(data_name,
                                                               username,
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

    def upload_policy(self, username, policy: Policy):
        """
        Uploads a policy written by the given user to DS

        Parameters:
         username: the unique username identifying which user wrote the policy
         policy: policy to upload

        Returns:
         Response of policy broker
        """

        if self.trust_mode == "full_trust":
            response = policy_broker.upload_policy(policy,
                                                   username,)
        else:
            response = policy_broker.upload_policy(policy,
                                                   username,
                                                   self.write_ahead_log,
                                                   self.key_manager,)

        return Response(status=response.status, message=response.message)

    def bulk_upload_policies(self, username, policies):
        """
        For testing purposes.
        """
        # This code here is unpolished. Just for testing purposes.
        if self.trust_mode == "full_trust":
            response = database_api.bulk_upload_policies(policies)
            return response
        else:
            response = policy_broker.bulk_upload_policies(policies,
                                                          username,
                                                          self.write_ahead_log,
                                                          self.key_manager,)
            return response

    def remove_policy(self, username, policy: Policy):
        """
        Removes a policy from DS

        Parameters:
         username: the unique username identifying which user wrote the policy
         policy: policy to remove

        Returns:
         Response of policy broker
        """

        if self.trust_mode == "full_trust":
            response = policy_broker.remove_policy(policy,
                                                   username,)
        else:
            response = policy_broker.remove_policy(policy,
                                                   username,
                                                   self.write_ahead_log,
                                                   self.key_manager,)

        return Response(status=response.status, message=response.message)

    @staticmethod
    def get_all_policies():
        """
        Gets all a policies from DS

        Parameters:

        Returns:
         ALL policies from policy broker
        """

        return policy_broker.get_all_policies()

    def suggest_share(self, username, agents: list[int], functions: list[str], data_elements: list[int]):
        """
        Propose a share. This leads to the creation of a share.

        Parameters:
            username: the unique username identifying which user is calling the api
            agents: list of user ids
            functions: list of functions
            data_elements: list of data elements
        """
        # We first register the share in the DB
        # Decide which share_id to use from self.cur_share_id
        share_id = self.cur_share_id
        self.cur_share_id += 1

        if self.trust_mode == "full_trust":
            response = share_manager.register_share_in_DB(username,
                                                          share_id, )
        else:
            response = share_manager.register_share_in_DB(username,
                                                          share_id,
                                                          self.write_ahead_log,
                                                          self.key_manager, )

        # We now create the policies with status 0.
        for a in agents:
            for f in functions:
                for d in data_elements:
                    cur_policy = Policy(user_id=a, api=f, data_id=d, share_id=share_id, status=0)
                    if self.trust_mode == "full_trust":
                        response = policy_broker.upload_policy(cur_policy,
                                                               username, )
                    else:
                        response = policy_broker.upload_policy(cur_policy,
                                                               username,
                                                               self.write_ahead_log,
                                                               self.key_manager, )
        return 0

    def ack_data_in_share(self, username, data_id, share_id):
        """
        Updates a policy's status to ready (1)

        Parameters:
            username: the unique username identifying which user is calling the api
            share_id: id of the share
            data_id: id of the data element
        """
        if self.trust_mode == "full_trust":
            response = policy_broker.ack_data_in_share(username, data_id, share_id)
        else:
            response = policy_broker.ack_data_in_share(username,
                                                       data_id,
                                                       share_id,
                                                       self.write_ahead_log,
                                                       self.key_manager, )

        return response

    def call_api(self, username, api: API, share_id=None, exec_mode=None, *args, **kwargs):
        """
        Calls an API as the given user

        Parameters:
         username: the unique username identifying which user is calling the api
         api: api to call
         share_id: from which share is this being called
         exec_mode: optimistic or pessimistic
         *args, **kwargs: arguments to the API call

        Returns:
         Response of data register
        """

        # get caller's UID
        cur_user = database_api.get_user_by_user_name(
            User(user_name=username, ))
        # If the user doesn't exist, something is wrong
        if cur_user.status == -1:
            print("Something wrong with the current user")
            return Response(status=1, message="Something wrong with the current user")
        cur_user_id = cur_user.data[0].id

        # Now we need to check if the current API called is a api_endpoint (non-jail) or a function (jail)
        # If it's non-jail, it does not need to go through the gatekeeper
        list_of_api_endpoint = get_registered_api_endpoint()
        for cur_api in list_of_api_endpoint:
            if api == cur_api.__name__:
                print("user is calling an api_endpoint", api)
                cur_api(*args, **kwargs)
                return 0

        # If it's jail, it goes to the gatekeeper
        res = self.gatekeeper.call_api(api,
                                       cur_user_id,
                                       share_id,
                                       exec_mode,
                                       *args,
                                       **kwargs)
        # Only when the returned status is 0 can we release the result
        if res.status == 0:
            api_result = res.result
            # We still need to encrypt the results using the caller's symmetric key if in no_trust_mode.
            if self.trust_mode == "no_trust":
                caller_symmetric_key = self.key_manager.get_agent_symmetric_key(
                    cur_user_id)
                api_result = cu.encrypt_data_with_symmetric_key(
                    cu.to_bytes(api_result), caller_symmetric_key)
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
                caller_symmetric_key = self.key_manager.get_agent_symmetric_key(
                    cur_user_id)
                api_result = cu.encrypt_data_with_symmetric_key(
                    cu.to_bytes(api_result), caller_symmetric_key)

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

    # def call_api(self, username, api: API, exec_mode, *args, **kwargs):
    #     """
    #     Calls an API as the given user
    #
    #     Parameters:
    #      username: the unique username identifying which user is calling the api
    #      api: api to call
    #      exec_mode: optimistic or pessimistic
    #      *args, **kwargs: arguments to the API call
    #
    #     Returns:
    #      Response of data register
    #     """
    #
    #     # get caller's UID
    #     cur_user = database_api.get_user_by_user_name(
    #         User(user_name=username, ))
    #     # If the user doesn't exist, something is wrong
    #     if cur_user.status == -1:
    #         print("Something wrong with the current user")
    #         return Response(status=1, message="Something wrong with the current user")
    #     cur_user_id = cur_user.data[0].id
    #
        # res = self.gatekeeper.call_api(api,
        #                                cur_user_id,
        #                                exec_mode,
        #                                *args,
        #                                **kwargs)
        # # Only when the returned status is 0 can we release the result
        # if res.status == 0:
        #     api_result = res.result
        #     # We still need to encrypt the results using the caller's symmetric key if in no_trust_mode.
        #     if self.trust_mode == "no_trust":
        #         caller_symmetric_key = self.key_manager.get_agent_symmetric_key(
        #             cur_user_id)
        #         api_result = cu.encrypt_data_with_symmetric_key(
        #             cu.to_bytes(api_result), caller_symmetric_key)
        #     return api_result
        # # In this case we need to put result into staging storage, so that they can be released later
        # elif res.status == -1:
        #     api_result = res.result[0]
        #     data_ids_accessed = res.result[1]
        #     # We first convert api_result to bytes because we need to store it in staging storage
        #     # In full_trust mode, we convert it to bytes directly
        #     if self.trust_mode == "full_trust":
        #         api_result = cu.to_bytes(api_result)
        #     # In no_trust mode, we encrypt it using caller's symmetric key
        #     else:
        #         caller_symmetric_key = self.key_manager.get_agent_symmetric_key(
        #             cur_user_id)
        #         api_result = cu.encrypt_data_with_symmetric_key(
        #             cu.to_bytes(api_result), caller_symmetric_key)
        #
        #     # print(api_result)
        #     # print(data_ids_accessed)
        #
        #     # Call staging storage to store the bytes
        #
        #     # Decide which data_id to use from ClientAPI.cur_data_id field
        #     staging_data_id = self.cur_staging_data_id
        #     self.cur_staging_data_id += 1
        #
        #     staging_storage_response = self.staging_storage.store(staging_data_id,
        #                                                           api_result)
        #     if staging_storage_response.status == 1:
        #         return staging_storage_response
        #
        #     # Storing into staging storage is successful. We now call data_register to register this staging DE in DB.
        #     # We need to store to both the staged table and the provenance table.
        #
        #     # Full_trust mode
        #     if self.trust_mode == "full_trust":
        #         # Staged table
        #         data_register_response_staged = data_register.register_staged_in_DB(staging_data_id,
        #                                                                             cur_user_id,
        #                                                                             api,)
        #         # Provenance table
        #         data_register_response_provenance = data_register.register_provenance_in_DB(staging_data_id,
        #                                                                                     data_ids_accessed,)
        #     else:
        #         # Staged table
        #         data_register_response_staged = data_register.register_staged_in_DB(staging_data_id,
        #                                                                             cur_user_id,
        #                                                                             api,
        #                                                                             self.write_ahead_log,
        #                                                                             self.key_manager,
        #                                                                             )
        #         # Provenance table
        #         data_register_response_provenance = data_register.register_provenance_in_DB(staging_data_id,
        #                                                                                     data_ids_accessed,
        #                                                                                     cur_user_id,
        #                                                                                     self.write_ahead_log,
        #                                                                                     self.key_manager,
        #                                                                                     )
        #     if data_register_response_staged.status != 0 or data_register_response_provenance.status != 0:
        #         return Response(status=data_register_response_staged.status,
        #                         message="internal database error")
        #     res_msg = "Staged data ID " + str(staging_data_id)
        #     return res_msg
        # else:
        #     return res.message

    # data users gives a staged DE ID and tries to release it
    def release_staged_DE(self, username, staged_ID):
        """
        Releases the staged data element. This function is run after call_api, on optimistic mode
         to release any staged data elements that aren't allowed to be shared.

        Parameters:
         username: the unique username identifying which user is calling the api
         api: api to call
         exec_mode: optimistic or pessimistic
         *args, **kwargs: arguments to the API call

        Returns:
         released data
        """

        # get caller's UID
        cur_user = database_api.get_user_by_user_name(
            User(user_name=username, ))
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

    def print_full_log(self):
        self.data_station_log.read_full_log(self.key_manager)

    # print out the contents of the WAL

    def print_wal(self):
        self.write_ahead_log.read_wal(self.key_manager)

    def recover_db_from_wal(self):
        """
        Recovers the database from the WAL

        Parameters:
         Nothing

        Returns:
         Nothing
        """

        # Step 1: reconstruct the DB
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
        """
        For testing purposes
        """

        with open("symmetric_keys.pkl", 'ab') as keys:
            agents_symmetric_key = pickle.dumps(
                self.key_manager.agents_symmetric_key)
            keys.write(agents_symmetric_key)

    # For testing purposes: read keys from a file

    def load_symmetric_keys(self):
        """
        For testing purposes
        """
        with open("symmetric_keys.pkl", "rb") as keys:
            agents_symmetric_key = pickle.load(keys)
            self.key_manager.agents_symmetric_key = agents_symmetric_key

    def shut_down(self):
        """
        Shuts down the DS system. Unmounts the interceptor and stops the process, clears
         the DB, app register, and db.checkpoint
        """
        # # print("shutting down...")
        # mount_point = self.config.mount_point
        # # print(mount_point)
        # unmount_status = os.system("umount " + str(mount_point))
        # counter = 0
        # while unmount_status != 0:
        #     time.sleep(1)
        #     unmount_status = os.system("umount " + str(mount_point))
        #     if counter == 10:
        #         print("Unmount failed")
        #         exit(1)
        #
        # assert os.path.ismount(mount_point) is False
        # self.interceptor_process.join()

        # Clear DB, app register, and db.checkpoint
        engine.dispose()
        clear_function()
        clear_api_endpoint()
        clear_checkpoint_table_paths()

        print("shut down complete")


if __name__ == "__main__":
    print("Main")
    # First parse test_config files and command line arguments

    # https://docs.python.org/3/library/argparse.html
    # (potentiall need to re-write some key-values from clg)
    data_station_config = parse_config(sys.argv[1])
    app_connector_config = parse_config(sys.argv[2])
    ds = DataStation(data_station_config, app_connector_config)

    # run_system(parse_config("data_station_config.yaml"))
