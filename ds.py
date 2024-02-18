import os
import sys
import multiprocessing
import pathlib
import time
import pickle

from common import common_procedure
from storagemanager.storage_manager import StorageManager
from demanager import de_manager
from contractmanager import contract_manager
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
from agentmanager import agent_manager
from functionmanager import function_manager
from common.abstraction import DataElement
from common.config import DSConfig


class DataStation:

    def __init__(self, ds_config, need_to_recover=False):
        """
        The general class that creates and initializes a Data Station.

        Parameters:
         ds_config: dictionary of general config options
         need_to_recover: bool that specifies if needs to recover from previous database
        """
        # parse config file
        self.config = DSConfig(ds_config)

        # set up development mode
        self.development_mode = self.config.development_mode

        # for development mode
        self.accessible_de_development = None

        # set up trust mode
        self.trust_mode = self.config.trust_mode

        # set up an instance of the storage_manager
        self.storage_path = self.config.storage_path
        self.storage_manager = StorageManager(self.storage_path)

        # set up an instance of the log
        log_in_memory_flag = self.config.log_in_memory_flag
        log_path = self.config.log_path
        self.data_station_log = Log(
            log_in_memory_flag, log_path, self.trust_mode)

        # set up an instance of WAL and KeyManager
        wal_path = self.config.wal_path
        check_point_freq = self.config.check_point_freq
        if self.trust_mode == "no_trust":
            self.write_ahead_log = WAL(wal_path, check_point_freq)
            self.key_manager = KeyManager()
        else:
            self.write_ahead_log = None
            self.key_manager = None

        # print(self.storage_path)
        self.epf_path = self.config.epf_path

        # register all api_endpoints
        register_epf(self.epf_path)

        # set up the gatekeeper
        self.gatekeeper = Gatekeeper(
            self.data_station_log,
            self.write_ahead_log,
            self.key_manager,
            self.trust_mode,
            self.epf_path,
            self.config,
            # self.config.ds_storage_path,
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

        # Decide which de_id to use at new insertion
        de_id_resp = database_api.get_de_with_max_id()
        if de_id_resp["status"] == 0:
            self.cur_de_id = de_id_resp["data"].id + 1
        else:
            self.cur_de_id = 1
        # print("Starting DE id should be:")
        # print(self.cur_de_id)

        # Decide which user_id to use at new insertion
        user_id_resp = database_api.get_user_with_max_id()
        if user_id_resp["status"] == 0:
            self.cur_user_id = user_id_resp["data"].id + 1
        else:
            self.cur_user_id = 1

        # Decide which contract_id to use at new insertion
        contract_id_resp = database_api.get_contract_with_max_id()
        if contract_id_resp["status"] == 0:
            self.cur_contract_id = contract_id_resp["data"].id + 1
        else:
            self.cur_contract_id = 1

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
        response = agent_manager.create_user(user_id,
                                             username,
                                             password,
                                             self.write_ahead_log,
                                             self.key_manager, )

        if response["status"] == 1:
            return response

        # Part 3: Creating a staging storage folder for the newly created user
        self.storage_manager.create_staging_for_user(user_id)

        return response

    def list_all_agents(self,
                        user_id):
        """
        Show all agents' (ID, name) in the current instance
        """
        agent_manager_response = agent_manager.list_all_agents()
        return agent_manager_response

    def register_de(self,
                    user_id,
                    de_name,
                    de_type,
                    access_param):
        """
        Registers a data element in Data Station's database.

        Parameters:
            user_id: the unique id identifying which user owns the de
            de_name: name of DE
            de_type: what types of data can be uploaded?
            access_param: additional parameters needed for acccessing the DE
        """
        # Decide which de_id to use from self.cur_de_id
        de_id = self.cur_de_id
        self.cur_de_id += 1

        return de_manager.register_de_in_DB(de_id,
                                            de_name,
                                            user_id,
                                            de_type,
                                            access_param,
                                            self.write_ahead_log,
                                            self.key_manager)

    def upload_de(self,
                  user_id,
                  de_id,
                  data_in_bytes):
        """
        Upload a file corresponding to a registered DE.

        Parameters:
            user_id: user id
            de_id: id of this existing DE
            data_in_bytes: plaintext data in bytes
        """
        # Check if DE exists, and whether its owner is the caller
        verify_owner_response = common_procedure.verify_de_owner(de_id, user_id)
        if verify_owner_response["status"] == 1:
            return verify_owner_response

        # We now get the de_name and de_type from de_id
        de_res = database_api.get_de_by_id(de_id)
        if de_res["status"] == 1:
            return de_res

        # If in no_trust mode, encrypt the DE using agent's symmetric key
        if self.trust_mode == "no_trust":
            data_in_bytes = cu.encrypt_data_with_symmetric_key(data_in_bytes,
                                                               self.key_manager.agents_symmetric_key[user_id])

        return self.storage_manager.store(de_res["data"].name,
                                          de_id,
                                          data_in_bytes,
                                          de_res["data"].type, )

    def remove_de_from_storage(self, user_id, de_id):
        """
        Remove a DE from storage. (Does not remove it from DB).
        """
        # Check if DE exists, and whether its owner is the caller
        verify_owner_response = common_procedure.verify_de_owner(de_id, user_id)
        if verify_owner_response["status"] == 1:
            return verify_owner_response

        return self.storage_manager.remove_de_from_storage(de_id)

    def remove_de_from_db(self, user_id, de_id):
        """
        Removes a DE from DS's database.
        """
        # Check if DE exists, and whether its owner is the caller
        verify_owner_response = common_procedure.verify_de_owner(de_id, user_id)
        if verify_owner_response["status"] == 1:
            return verify_owner_response

        # First call de_manager to remove the DE from database
        de_manager_response = de_manager.remove_de_from_db(user_id,
                                                           de_id,
                                                           self.write_ahead_log,
                                                           self.key_manager, )
        if de_manager_response["status"] == 1:
            return de_manager_response

        # At this step we have removed the record about the de from DB
        # Now we remove its actual content from storage.
        storage_manager_response = self.storage_manager.remove_de_from_storage(de_id)

        # If DE exists in storage, but removal failed
        if storage_manager_response["status"] == 1:
            return storage_manager_response

        return de_manager_response

    def list_all_des_with_src(self, user_id):
        """
        List IDs of all des with the source agent IDs.

        Parameters:
            user_id: id of caller
        """
        return de_manager.list_all_des_with_src()

    def get_all_functions(self, user_id):
        """
        List names of all functions.
        """
        return function_manager.get_all_functions()

    def get_function_info(self, user_id, function_name):
        """
        Return docstring of given function.
        """
        return function_manager.get_function_info(function_name)

    def propose_contract(self,
                         user_id,
                         dest_agents,
                         data_elements,
                         function,
                         status,
                         *args,
                         **kwargs):
        """
        Propose a contract. This leads to the creation of a contract.

        Parameters:
            user_id: the unique id identifying which user is calling the api
            dest_agents: list of user ids
            data_elements: list of data elements
            function: function
            status: default approval status of a contract (for CMP specification)
            args: args to the template function
            kwargs: kwargs to the template function

        Returns:
        A response object with the following fields:
            status: status of proposing a contract. 0: success, 1: failure.
        """
        # We first register the contract in the DB
        # Decide which contract_id to use from self.cur_contract_id
        contract_id = self.cur_contract_id
        self.cur_contract_id += 1

        return contract_manager.register_contract_in_DB(user_id,
                                                        contract_id,
                                                        dest_agents,
                                                        data_elements,
                                                        function,
                                                        status,
                                                        self.write_ahead_log,
                                                        self.key_manager,
                                                        *args,
                                                        **kwargs, )

    def show_contract(self, user_id, contract_id):
        """
        Display the content of a contract.

        Parameters:
            user_id: id of caller
            contract_id: id of the contract that the caller wants to see

        Returns:
        An object with the following fields:
            a_dest: a list of ids of the destination agents
            de: a list of ids of the data elements
            template: which template function
            args: arguments to the template function
            kwargs: kwargs to the template function
        """
        return contract_manager.show_contract(user_id, contract_id)

    def show_all_contracts_as_dest(self, user_id):
        """
        Display all contracts, for which caller is a destination agent.
        """
        return contract_manager.show_all_contracts_as_dest(user_id)

    def show_all_contracts_as_src(self, user_id):
        """
        Display all contracts, for which caller is an approval agent.
        """
        return contract_manager.show_all_contracts_as_src(user_id)

    def approve_contract(self, user_id, contract_id):
        """
        Update a contract's status to approved (1), for source agent <user_id>.

        Parameters:
            user_id: caller username
            contract_id: id of the contract

        Returns:
        A response object with the following fields:
            status: status of approving contract. 0: success, 1: failure.
        """
        return contract_manager.approve_contract(user_id,
                                                 contract_id,
                                                 self.write_ahead_log,
                                                 self.key_manager, )

    def reject_contract(self, user_id, contract_id):
        """
        Update a contract's status to rejected (1), for source agent <user_id>.

        Parameters:
            user_id: caller username
            contract_id: id of the contract

        Returns:
        A response object with the following fields:
        A response object with the following fields:
            status: status of rejecting contract. 0: success, 1: failure.
        """
        return contract_manager.reject_contract(user_id,
                                                contract_id,
                                                self.write_ahead_log,
                                                self.key_manager, )

    def execute_contract(self, user_id, contract_id):
        """
        Execute a contract.

        Parameters:
            user_id: caller id
            contract_id: id of the contract

        Returns:
            The result of executing the contract (f(P))
        """

        # fetch arguments
        function, function_param = contract_manager.get_contract_function_and_param(contract_id)
        args = function_param["args"]
        kwargs = function_param["kwargs"]

        print("Function called in contract is", function)

        # Case 1: in development mode, we mimic the behaviour of Gatekeeper
        if self.development_mode:
            # Check destination agent
            dest_a_ids = contract_manager.get_dest_ids_for_contract(contract_id)
            if int(user_id) not in dest_a_ids:
                print("Caller not a destination agent")
                return None

            # Check contract status
            contract_ready_flag = contract_manager.check_contract_ready(contract_id)
            if not contract_ready_flag:
                print("This contract's status is not approved.")
                return None

            # Get accessible data elements
            all_accessible_de_id = contract_manager.get_de_ids_for_contract(contract_id)
            # print(f"all accessible data elements are: {all_accessible_de_id}")

            get_des_by_ids_res = database_api.get_des_by_ids(all_accessible_de_id)
            if get_des_by_ids_res["status"] == 1:
                print("Something wrong with getting accessible DE for contract.")
                return get_des_by_ids_res

            accessible_de = set()
            for cur_data in get_des_by_ids_res["data"]:
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

            print(self.accessible_de_development)

            # Execute function
            list_of_function = get_registered_functions()

            for cur_fn in list_of_function:
                if function == cur_fn.__name__:
                    print("Calling a function in development:", function)
                    print("Args is", args)
                    print("Kwargs is ", kwargs)
                    res = cur_fn(*args, **kwargs)
                    # Writing the result to caller's staging storage, under file name {contract_id}.
                    # self.write_staged(contract_id, user_id, res)
                    return res

            # Getting here means called function is not found
            print("Called function does not exist")
            return None
        else:
            # Case 2: Sending to Gatekeeper
            print("Calling a function in docker:", function)
            res = self.gatekeeper.call_api(function,
                                           user_id,
                                           contract_id,
                                           *args,
                                           **kwargs)
            return res

    def call_api(self, username, api, *args, **kwargs):
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
        user_resp = database_api.get_user_by_user_name(username)
        if user_resp["status"] == 1:
            return user_resp
        user_id = user_resp["data"].id

        # First we check if we are in development mode, if true, call call_api_development
        if self.development_mode:
            res = self.call_api_development(user_id, api, *args, **kwargs)
            return res

        # Calling the actual API endpoint
        list_of_api_endpoint = get_registered_api_endpoint()
        for cur_api in list_of_api_endpoint:
            if api == cur_api.__name__:
                print("user is calling an api_endpoint", api)
                # print(args)
                res = cur_api(user_id, *args, **kwargs)
                return res

        print("Called api_endpoint not found:", api)
        return None

    def write_staged(self, file_name, user_id, content):
        """
        Writes to staging storage in full trust mode.
        If file exists, its content will be overwritten.
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
            print("Called API endpoint not found in EPM:", api)
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
        if user_id_resp["status"] == 0:
            self.cur_user_id = user_id_resp["data"].id + 1
        else:
            self.cur_user_id = 1
        print("User ID to use after recovering DB is: " + str(self.cur_user_id))

        # Step 3: reset self.cur_de_id from DB
        de_id_resp = database_api.get_de_with_max_id()
        if de_id_resp["status"] == 0:
            self.cur_de_id = de_id_resp["data"].id + 1
        else:
            self.cur_de_id = 1
        print("DE ID to use after recovering DB is: " + str(self.cur_de_id))

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
