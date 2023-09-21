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
from common import common_procedure
from common.general_utils import parse_config

from storagemanager.storage_manager import StorageManager
from stagingstorage.staging_storage import StagingStorage
from policybroker import policy_broker
from demanager import de_manager
from sharemanager import share_manager
from verifiability.log import Log
from writeaheadlog.write_ahead_log import WAL
from crypto import cryptoutils as cu
from crypto.key_manager import KeyManager
from gatekeeper.gatekeeper import Gatekeeper
from dbservice import database_api
from dbservice.database import engine
from dsapplicationregistration.dsar_core import (get_registered_api_endpoint,
                                                 get_registered_functions,
                                                 clear_api_endpoint,
                                                 clear_function,
                                                 register_epf, )
from userregister import user_register
from common.abstraction import DataElement


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

            # development mode
            self.development_mode = ds_config["in_development_mode"]

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

        # set up development mode
        self.development_mode = self.config.development_mode

        # for development mode
        self.accessible_de_development = None

        # set up trust mode
        self.trust_mode = self.config.trust_mode

        # set up an instance of the storage_manager
        self.storage_path = self.config.storage_path
        self.storage_manager = StorageManager(self.storage_path)

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

        # set up an instance of the key manager
        self.key_manager = KeyManager()

        # print(self.storage_path)
        self.epf_path = app_config["epf_path"]

        # register all api_endpoints
        register_epf(self.epf_path)

        # set up the gatekeeper
        self.gatekeeper = Gatekeeper(
            self.data_station_log,
            self.write_ahead_log,
            self.key_manager,
            self.trust_mode,
            self.epf_path,
            self.config.ds_storage_path,
            self.development_mode,
        )

        # set up the table_paths in dbservice.check_point
        table_paths = self.config.table_paths
        database_api.set_checkpoint_table_paths(table_paths)

        # Lastly, if we are in recover mode, we need to recover the DS database
        if need_to_recover:
            self.load_symmetric_keys()
            database_api.recover_db_from_snapshots(self.key_manager)
            self.recover_db_from_wal()

        # Decide which data_id to use at new insertion
        data_id_resp = database_api.get_data_with_max_id()
        if data_id_resp.status == 1:
            self.cur_data_id = data_id_resp.data[0].id + 1
        else:
            self.cur_data_id = 1
        # print("Starting data id should be:")
        # print(self.cur_data_id)

        # Decide which staging_data_id to use at new insertion
        staging_id_resp = database_api.get_staging_with_max_id()
        if staging_id_resp.status == 1:
            self.cur_staging_data_id = staging_id_resp.data[0].id + 1
        else:
            self.cur_staging_data_id = 1

        # Decide which user_id to use at new insertion
        user_id_resp = database_api.get_user_with_max_id()
        if user_id_resp.status == 1:
            self.cur_user_id = user_id_resp.data[0].id + 1
        else:
            self.cur_user_id = 1

        # Decide which share_id to use at new insertion
        share_id_resp = database_api.get_share_with_max_id()
        if share_id_resp.status == 1:
            self.cur_share_id = share_id_resp.data[0].id + 1
        else:
            self.cur_share_id = 1

    def create_user(self, username, password, user_sym_key=None, user_public_key=None):
        """
        Creates a user in DS

        Parameters:
         username: username
         password: password
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
                                                 username,
                                                 password, )
        else:
            response = user_register.create_user(user_id,
                                                 username,
                                                 password,
                                                 self.write_ahead_log,
                                                 self.key_manager, )

        if response.status == 1:
            return Response(status=response.status, message=response.message)

        # Part 3: Creating a staging storage folder for the newly created user
        self.storage_manager.create_staging_for_user(user_id)

        return response

    def get_all_apis(self):
        """
        Gets all APIs from the policy broker

        Parameters:
         Nothing

        Returns:
         all apis
        """
        # Call policy_broker directly
        return policy_broker.get_all_apis()

    def get_all_api_dependencies(self):
        """
        Gets all API dependencies from the policy broker

        Parameters:
         Nothing

        Returns:
         all dependencies
        """

        # Call policy_broker directly
        return policy_broker.get_all_dependencies()

    def register_de(self,
                    user_id,
                    data_name,
                    data_type,
                    access_param,
                    optimistic):
        """
        Registers a data element in Data Station's database.

        Parameters:
            user_id: the unique id identifying which user owns the dataset
            data_name: name of the data
            data_type: what types of data can be uploaded?
            optimistic: flag to be included in optimistic data discovery
            access_param: additional parameters needed for acccessing the DE
        """
        # Decide which data_id to use from ClientAPI.cur_data_id field
        data_id = self.cur_data_id
        self.cur_data_id += 1

        if self.trust_mode == "full_trust":
            de_manager_response = de_manager.register_data_in_DB(data_id,
                                                                 data_name,
                                                                 user_id,
                                                                 data_type,
                                                                 access_param,
                                                                 optimistic)
        else:
            de_manager_response = de_manager.register_data_in_DB(data_id,
                                                                 data_name,
                                                                 user_id,
                                                                 data_type,
                                                                 access_param,
                                                                 optimistic,
                                                                 self.write_ahead_log,
                                                                 self.key_manager)
        return de_manager_response

    def upload_de(self,
                  user_id,
                  data_id,
                  data_in_bytes):
        """
        Upload a file corresponding to a registered DE.

        Parameters:
            username: the unique username identifying which user owns the dataset
            data_id: id of this existing DE
            data_in_bytes: daat in bytes
        """
        # Check if the dataset exists, and whether data owner is the current user
        verify_owner_response = common_procedure.verify_dataset_owner(data_id, user_id)
        if verify_owner_response.status == 1:
            return verify_owner_response

        # We now get the data_name and data_type from data_id
        data_res = database_api.get_data_by_id(data_id)
        if data_res.status == -1:
            return data_res

        storage_manager_response = self.storage_manager.store(data_res.data[0].name,
                                                              data_id,
                                                              data_in_bytes,
                                                              data_res.data[0].type, )
        return storage_manager_response

    def remove_dataset(self, username, data_name):
        """
        Removes a dataset from DS

        Parameters:
         username: the unique username identifying which user owns the dataset
         data_name: name of the data

        Returns:
         Response of data register
        """

        # First we call de_manager to remove the existing dataset from the database
        if self.trust_mode == "full_trust":
            de_manager_response = de_manager.remove_data(data_name,
                                                         username, )
        else:
            de_manager_response = de_manager.remove_data(data_name,
                                                         username,
                                                         self.write_ahead_log,
                                                         self.key_manager, )
        if de_manager_response.status != 0:
            return Response(status=de_manager_response.status, message=de_manager_response.message)

        # At this step we have removed the record about the dataset from DB
        # Now we remove its actual content from SM
        storage_manager_response = self.storage_manager.remove(data_name,
                                                               de_manager_response.data_id,
                                                               de_manager_response.type, )

        # If SM removal failed
        if storage_manager_response.status == 1:
            return storage_manager_response

        return Response(status=de_manager_response.status, message=de_manager_response.message)

    def list_discoverable_des(self, user_id):
        """
        List IDs of all des in discoverable mode.

        Parameters:
            user_id: id of user

        Returns:
        A list containing IDs of all discoverable des.
        """
        de_manager_response = de_manager.list_discoverable_des()
        return de_manager_response

    def upload_policy(self, username, user_id, api, data_id, share_id):
        """
        Uploads a policy written by the given user to DS

        Parameters:
            username: the unique username identifying which user wrote the policy
            user_id: part of policy to upload, the user ID of the policy
            api: the api the policy refers to
            data_id: the data id the policy refers to
            share_id: to which share does this policy apply.

        Returns:
            Response of policy broker
        """
        policy = Policy(user_id=user_id, api=api, data_id=data_id, share_id=share_id, status=1)

        if self.trust_mode == "full_trust":
            response = policy_broker.upload_policy(policy,
                                                   username, )
        else:
            response = policy_broker.upload_policy(policy,
                                                   username,
                                                   self.write_ahead_log,
                                                   self.key_manager, )

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
                                                          self.key_manager, )
            return response

    def remove_policy(self, username, user_id, api, data_id):
        """
        Removes a policy from DS

        Parameters:
         username: the unique username identifying which user wrote the policy
         user_id: part of policy to upload, the user ID of the policy
         api: the api the policy refers to
         data_id: the data id the policy refers to

        Returns:
         Response of policy broker
        """

        policy = Policy(user_id=user_id, api=api, data_id=data_id)

        if self.trust_mode == "full_trust":
            response = policy_broker.remove_policy(policy,
                                                   username, )
        else:
            response = policy_broker.remove_policy(policy,
                                                   username,
                                                   self.write_ahead_log,
                                                   self.key_manager, )

        return Response(status=response.status, message=response.message)

    def get_all_policies(self):
        """
        Gets all a policies from DS

        Parameters:

        Returns:
         ALL policies from policy broker
        """

        return policy_broker.get_all_policies()

    def suggest_share(self,
                      user_id,
                      dest_agents,
                      data_elements,
                      template,
                      *args,
                      **kwargs):
        """
        Propose a share. This leads to the creation of a share.

        Parameters:
            user_id: the unique id identifying which user is calling the api
            dest_agents: list of user ids
            data_elements: list of data elements
            template: template function
            args: args to the template function
            kwargs: kwargs to the template function

        Returns:
        A response object with the following fields:
            status: status of suggesting share. 0: success, 1: failure.
        """
        # We first register the share in the DB
        # Decide which share_id to use from self.cur_share_id
        share_id = self.cur_share_id
        self.cur_share_id += 1

        if self.trust_mode == "full_trust":
            response = share_manager.register_share_in_DB(share_id,
                                                          dest_agents,
                                                          data_elements,
                                                          template,
                                                          *args,
                                                          **kwargs, )
        else:
            response = share_manager.register_share_in_DB_no_trust(user_id,
                                                                   share_id,
                                                                   dest_agents,
                                                                   data_elements,
                                                                   template,
                                                                   self.write_ahead_log,
                                                                   self.key_manager,
                                                                   *args,
                                                                   **kwargs, )

        return share_id

    def show_share(self, user_id, share_id):
        """
        Display the content of a share.

        Parameters:
            user_id: id of caller
            share_id: id of the share that the caller wants to see

        Returns:
        An object with the following fields:
            a_dest: a list of ids of the destination agents
            de: a list of ids of the data elements
            template: which template function
            args: arguments to the template function
            kwargs: kwargs to the template function
        """
        return share_manager.show_share(user_id, share_id)

    def approve_share(self, user_id, share_id):
        """
        Update a share's status to ready, for approval agent <username>.

        Parameters:
            user_id: caller username
            share_id: id of the share

        Returns:
        A response object with the following fields:
            status: status of approving share. 0: success, 1: failure.
        """
        if self.trust_mode == "full_trust":
            return share_manager.approve_share(user_id,
                                               share_id, )
        else:
            return share_manager.approve_share(user_id,
                                               share_id,
                                               self.write_ahead_log,
                                               self.key_manager, )

    def execute_share(self, user_id, share_id):
        """
        Execute a share.

        Parameters:
            user_id: caller id
            share_id: id of the share

        Returns:
            The result of executing the share (f(P))
        """

        # fetch arguments
        share_template, share_param = share_manager.get_share_template_and_param(share_id)
        args = share_param["args"]
        kwargs = share_param["kwargs"]

        # Case 1: in development mode, we mimic the behaviour of Gatekeeper
        if self.development_mode:
            # Check destination agent
            dest_a_ids = share_manager.get_dest_ids_for_share(share_id)
            if user_id not in dest_a_ids:
                print("Caller not a destination agent")
                return None

            # Check share status
            share_ready_flag = share_manager.check_share_ready(share_id)
            if not share_ready_flag:
                print("This share has not been approved to execute yet.")
                return None

            # Get accessible data elements
            all_accessible_de_id = share_manager.get_de_ids_for_share(share_id)
            # print(f"all accessible data elements are: {all_accessible_de_id}")

            get_datasets_by_ids_res = database_api.get_datasets_by_ids(all_accessible_de_id)
            if get_datasets_by_ids_res.status == -1:
                print("Something wrong with getting accessible DE for share.")
                return None

            accessible_de = set()
            for cur_data in get_datasets_by_ids_res.data:
                if self.trust_mode == "no_trust":
                    data_owner_symmetric_key = self.key_manager.get_agent_symmetric_key(cur_data.owner_id)
                else:
                    data_owner_symmetric_key = None
                cur_de = DataElement(cur_data.id,
                                     cur_data.name,
                                     cur_data.type,
                                     cur_data.access_param,
                                     data_owner_symmetric_key)
                accessible_de.add(cur_de)

            for cur_de in accessible_de:
                if cur_de.type == "file":
                    cur_de.access_param = os.path.join(self.storage_path, cur_de.access_param)

            self.accessible_de_development = accessible_de

            # Execute share
            list_of_function = get_registered_functions()

            for cur_fn in list_of_function:
                if share_template == cur_fn.__name__:
                    print("Calling a template function in development", share_template)
                    print(args)
                    print(kwargs)
                    res = cur_fn(*args, **kwargs)
                    return res
        else:
            # Case 2: Sending to Gatekeeper
            res = self.gatekeeper.call_api(share_template,
                                           user_id,
                                           share_id,
                                           "pessimistic",
                                           *args,
                                           **kwargs)
            return res

    def ack_data_in_share(self, username, data_id, share_id):
        """
        (Outdated)

        Parameters:
            username: the unique username identifying which user is calling the api
            share_id: id of the share
            data_id: id of the data element

        Returns:
        A response object with the following fields:
            status: status of acknowledging share. 0: success, 1: failure.
        """
        if self.trust_mode == "full_trust":
            response = policy_broker.ack_data_in_share(
                username, data_id, share_id)
        else:
            response = policy_broker.ack_data_in_share(username,
                                                       data_id,
                                                       share_id,
                                                       self.write_ahead_log,
                                                       self.key_manager, )

        return response

    def call_api(self, username, api: API, *args, **kwargs):
        """
        Calls an API as the given user

        Parameters:
         username: the unique username identifying which user is calling the api
         api: api to call
         *args, **kwargs: arguments to the API call

        Returns:
         result of calling the API
        """

        # get caller's UID
        cur_user = database_api.get_user_by_user_name(User(user_name=username, ))
        # If the user doesn't exist, something is wrong
        if cur_user.status == -1:
            print("Something wrong with the current user")
            return Response(status=1, message="Something wrong with the current user")
        user_id = cur_user.data[0].id

        # First we check if we are in development mode, if true, call call_api_development
        if self.development_mode:
            res = self.call_api_development(user_id, api, *args, **kwargs)
            return res

        # Now we need to check if the current API called is an api_endpoint (non-jail) or a function (jail)
        # If it's non-jail, it does not need to go through the gatekeeper
        list_of_api_endpoint = get_registered_api_endpoint()
        for cur_api in list_of_api_endpoint:
            if api == cur_api.__name__:
                print("user is calling an api_endpoint", api)
                # print(args)
                res = cur_api(user_id, *args, **kwargs)
                return res

    def write_staged(self, file_name, user_id, content):
        """
        Writes to staging storage in full trust mode.
        """
        with open(f"{self.storage_path}/Staging_storage/{user_id}/{file_name}", 'wb+') as f:
            f.write(pickle.dumps(content))

    def release_staged(self, user_id):
        """
        Releases all files in a user's staging storage.
        """
        all_contents = []
        cur_dir = f"{self.storage_path}/Staging_storage/{user_id}"
        staged_list = os.listdir(cur_dir)
        for staged in staged_list:
            cur_staged_path = os.path.join(cur_dir, staged)
            with open(cur_staged_path, "rb") as f:
                cur_content = pickle.load(f)
                all_contents.append(cur_content)
        return all_contents

    # # data users gives a staged DE ID and tries to release it
    # def release_staged_DE(self, username, staged_ID):
    #     """
    #     Releases the staged data element. This function is run after call_api, on optimistic mode
    #      to release any staged data elements that aren't allowed to be shared.
    #
    #     Parameters:
    #      username: the unique username identifying which user is calling the api
    #      staged_ID: id of the staged data element
    #
    #     Returns:
    #      released data
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
    #     # First get the API call that generated this staged DE
    #     api = database_api.get_api_for_staged_id(staged_ID)
    #
    #     # Then get the currently accessible DEs for the <cur_user_id, api> combo
    #     accessible_data_ids = policy_broker.get_user_api_info(cur_user_id, api)
    #
    #     # Then get the parent_ids for this staged DE (all accessed DEs used to create this DE)
    #     accessed_data_ids = database_api.get_parent_id_for_staged_id(staged_ID)
    #
    #     # Then check if accessed_data_ids is a subset of accessible_data_ids
    #     # If yes, we can release the staged DE
    #     if set(accessed_data_ids).issubset(set(accessible_data_ids)):
    #         res = cu.from_bytes(self.staging_storage.release(staged_ID))
    #         return res

    def print_full_log(self):
        self.data_station_log.read_full_log(self.key_manager)

    # print out the contents of the WAL

    def print_wal(self):
        self.write_ahead_log.read_wal(self.key_manager)

    def call_api_development(self, user_id, api, *args, **kwargs):
        """
        For testing: handling api calls in development mode
        """

        # Check if api called is valid (either a function or an api_endpoint)
        api_type = None
        list_of_api_endpoint = get_registered_api_endpoint()
        for cur_api in list_of_api_endpoint:
            if api == cur_api.__name__:
                api_type = "api_endpoint"
                break

        if api_type is None:
            print("Called API endpoint not found in EPM.")
            return None

        # Case 1: calling non-jail function
        for cur_api in list_of_api_endpoint:
            if api == cur_api.__name__:
                print(f"Calling api_endpoint in dev mode: {api}")
                res = cur_api(user_id, *args, **kwargs)
                return res

    def get_all_accessible_des(self):
        """
        For testing: when in development mode, fetches all DEs.

        Parameters:

        Returns:
            a list of DataElements
        """

        if not self.development_mode:
            return -1

        return self.accessible_de_development

    def get_de_by_id(self, de_id):
        """
        For testing: when in development mode, fetches DE by id.

        Parameters:
            de_id: id of the DataElement

        Returns:
            DataElement specified by de_ied
        """

        if not self.development_mode:
            print("Something is wrong. Shout not be here if not in dev mode.")
            return -1

        all_des = self.get_all_accessible_des()
        for de in all_des:
            if de.id == de_id:
                return de

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
        print("User ID to use after recovering DB is: " + str(self.cur_user_id))

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
         the DB, app register, and db.checkpoint, and shuts down the gatekeeper server
        """
        # stop gatekeeper server
        self.gatekeeper.shut_down()

        # Clear DB, app register, and db.checkpoint
        engine.dispose()
        clear_function()
        clear_api_endpoint()
        database_api.clear_checkpoint_table_paths()

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
