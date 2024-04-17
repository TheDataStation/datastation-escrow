import os
import sys
import multiprocessing
import pathlib
import time
import pickle
import json

from common import common_procedure
from storagemanager.storage_manager import StorageManager
from appstatemanager.app_state_manager import AppStateManager
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
        self.accessible_de_development = set()
        self.accessed_de_development = set()
        self.api_type_development = None

        # set up trust mode
        self.trust_mode = self.config.trust_mode

        # set up an instance of WAL and KeyManager
        wal_path = self.config.wal_path
        check_point_freq = self.config.check_point_freq
        if self.trust_mode == "no_trust":
            self.write_ahead_log = WAL(wal_path, check_point_freq)
            self.key_manager = KeyManager()
        else:
            self.write_ahead_log = None
            self.key_manager = None

        # set up an instance of the storage_manager
        self.storage_path = self.config.storage_path
        self.storage_manager = StorageManager(self.storage_path)

        # set up an instance of the app state manager
        self.app_state_path = self.config.app_state_path
        if self.trust_mode == "no_trust":
            self.app_state_manager = AppStateManager(self.app_state_path,
                                                     self.key_manager.agents_symmetric_key[0])
            self.app_state_manager = AppStateManager(self.app_state_path)
        else:
            self.app_state_manager = AppStateManager(self.app_state_path)

        # set up an instance of the log
        log_in_memory_flag = self.config.log_in_memory_flag
        log_path = self.config.log_path
        self.data_station_log = Log(
            log_in_memory_flag, log_path, self.trust_mode)

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

        # Keep track of calling agent ID
        self.caller_id = 0

    def create_agent(self, username, password, user_sym_key=None, user_public_key=None):
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
        response = agent_manager.create_agent(user_id,
                                              username,
                                              password,
                                              self.write_ahead_log,
                                              self.key_manager, )

        if response["status"] == 1:
            return response

        # Part 3: Creating a staging storage folder for the newly created user
        self.storage_manager.create_staging_for_user(user_id)

        return response

    def login_agent(self, username, password):
        """
        Agent login.
        """
        return agent_manager.login_agent(username, password)

    def list_all_agents(self):
        """
        Show all agents' (ID, name) in the current instance
        """
        agent_manager_response = agent_manager.list_all_agents()
        return agent_manager_response

    def register_de(self,
                    user_id,
                    store_type,
                    derived,
                    de_id=None):
        """
        Registers a data element in Data Station's database.

        Parameters:
            user_id: the unique id identifying which user owns the de
        """
        # Decide which de_id to use from self.cur_de_id
        if de_id is None:
            de_id = self.cur_de_id
            self.cur_de_id += 1

        return de_manager.register_de_in_DB(de_id,
                                            user_id,
                                            store_type,
                                            derived,
                                            self.write_ahead_log,
                                            self.key_manager)

    # def upload_de(self,
    #               user_id,
    #               de_id,
    #               data_in_bytes):
    #     """
    #     Upload a file corresponding to a registered DE.
    #
    #     Parameters:
    #         user_id: user id
    #         de_id: id of this existing DE
    #         data_in_bytes: plaintext data in bytes
    #     """
    #     # Check if DE exists, and whether its owner is the caller
    #     verify_owner_response = common_procedure.verify_de_owner(de_id, user_id)
    #     if verify_owner_response["status"] == 1:
    #         return verify_owner_response
    #
    #     # We now get the de_name and de_type from de_id
    #     de_res = database_api.get_de_by_id(de_id)
    #     if de_res["status"] == 1:
    #         return de_res
    #
    #     # If in no_trust mode, encrypt the DE using agent's symmetric key
    #     if self.trust_mode == "no_trust":
    #         data_in_bytes = cu.encrypt_data_with_symmetric_key(data_in_bytes,
    #                                                            self.key_manager.agents_symmetric_key[user_id])
    #
    #     return self.storage_manager.store(de_res["data"].name,
    #                                       de_id,
    #                                       data_in_bytes,
    #                                       de_res["data"].type, )

    def csv_store_write(self, content):
        # If called from development mode, it could be called from api_endpoint or function
        if self.api_type_development:
            # If it's an api_endpoint, set "derived" to False, and encrypt with caller's sym_key if needed
            if self.api_type_development == "api_endpoint":
                res = self.register_de(self.caller_id, "csv", False)
                if res["status"]:
                    return res
                if self.trust_mode == "no_trust":
                    content = cu.encrypt_data_with_symmetric_key(content,
                                                                 self.key_manager.agents_symmetric_key[self.caller_id])
                return self.storage_manager.write(res["de_id"], content, "csv")
            # If it's a function, we need to write an intermediate DE. We set its origin to accessed_de_dev.
            # Don't give it an owner, and encrypt it with DS sym key if needed.
            else:
                print("Source DE for this derived DE is", self.accessed_de_development)
                res = self.register_de(self.caller_id, "csv", True)
                if res["status"]:
                    return res
                for src_de_id in self.accessed_de_development:
                    if self.write_ahead_log:
                        wal_entry = f"database_api.create_derived_de({res['de_id']}, {src_de_id})"
                        self.write_ahead_log.log(self.caller_id, wal_entry, self.key_manager, )
                    db_res = database_api.create_derived_de(res["de_id"], src_de_id)
                    if db_res["status"] == 1:
                        return db_res
                # Encrypt with DS sym key if needed
                if self.trust_mode == "no_trust":
                    content = cu.encrypt_data_with_symmetric_key(content,
                                                                 self.key_manager.agents_symmetric_key[0])
                return self.storage_manager.write(res["de_id"], content, "csv")

        # Else, it can only be called from api_endpoint (the other implementation should be in EscrowAPIDocker)
        else:
            res = self.register_de(self.caller_id, "csv", False)
            if res["status"]:
                return res
            if self.trust_mode == "no_trust":
                content = cu.encrypt_data_with_symmetric_key(content,
                                                             self.key_manager.agents_symmetric_key[self.caller_id])
            return self.storage_manager.write(res["de_id"], content, "csv")

    def object_store_write(self, content):
        # If called from development mode, it could be called from api_endpoint or function
        if self.api_type_development:
            # If it's an api_endpoint, set "derived" to False, and encrypt with caller's sym_key if needed
            if self.api_type_development == "api_endpoint":
                res = self.register_de(self.caller_id, "object", False)
                if res["status"]:
                    return res
                pickled_bytes = pickle.dumps(content)
                if self.trust_mode == "no_trust":
                    pickled_bytes = cu.encrypt_data_with_symmetric_key(pickled_bytes,
                                                                       self.key_manager.agents_symmetric_key[
                                                                           self.caller_id])
                return self.storage_manager.write(res["de_id"], pickled_bytes, "object")
            else:
                print("Source DE for this derived DE is", self.accessed_de_development)
                res = self.register_de(self.caller_id, "object", True)
                if res["status"]:
                    return res
                for src_de_id in self.accessed_de_development:
                    if self.write_ahead_log:
                        wal_entry = f"database_api.create_derived_de({res['de_id']}, {src_de_id})"
                        self.write_ahead_log.log(self.caller_id, wal_entry, self.key_manager, )
                    db_res = database_api.create_derived_de(res["de_id"], src_de_id)
                    if db_res["status"] == 1:
                        return db_res
                # Encrypt with DS sym key if needed
                pickled_bytes = pickle.dumps(content)
                if self.trust_mode == "no_trust":
                    pickled_bytes = cu.encrypt_data_with_symmetric_key(pickled_bytes,
                                                                       self.key_manager.agents_symmetric_key[0])
                return self.storage_manager.write(res["de_id"], pickled_bytes, "object")
        # If not in development,
        # it can only be called from api_endpoint (the other implementation should be in EscrowAPIDocker)
        else:
            res = self.register_de(self.caller_id, "object", False)
            if res["status"]:
                return res
            pickled_bytes = pickle.dumps(content)
            if self.trust_mode == "no_trust":
                pickled_bytes = cu.encrypt_data_with_symmetric_key(pickled_bytes,
                                                                   self.key_manager.agents_symmetric_key[
                                                                       self.caller_id])
            return self.storage_manager.write(res["de_id"], pickled_bytes, "object")

    # All DEStore.read below will only be called in development mode. (Since DEs can only be accessed in sandbox)
    # If called outside of development mode, call to this function shoule be disabled
    def csv_store_read(self, de_id):
        self.accessed_de_development.add(de_id)
        return self.storage_manager.read(de_id, "csv")

    def object_store_read(self, de_id):
        self.accessed_de_development.add(de_id)
        pickled_bytes = self.storage_manager.read(de_id, "object")
        if self.trust_mode == "no_trust":
            pickled_bytes = cu.decrypt_data_with_symmetric_key(pickled_bytes,
                                                               self.key_manager.agents_symmetric_key[self.caller_id])
        return pickle.loads(pickled_bytes)

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

    def list_all_des_with_src(self):
        """
        List IDs of all des with the source agent IDs.
        """
        return de_manager.list_all_des_with_src()

    def get_all_functions(self):
        """
        List names of all functions.
        """
        return function_manager.get_all_functions()

    def get_function_info(self, function_name):
        """
        Return docstring of given function.
        """
        return function_manager.get_function_info(function_name)

    def propose_contract(self,
                         dest_agents,
                         data_elements,
                         function,
                         *args,
                         **kwargs):
        """
        Propose a contract. This leads to the creation of a contract.
        """
        # We first register the contract in the DB
        # Decide which contract_id to use from self.cur_contract_id
        contract_id = self.cur_contract_id

        res = contract_manager.propose_contract(self.caller_id,
                                                contract_id,
                                                dest_agents,
                                                data_elements,
                                                function,
                                                self.write_ahead_log,
                                                self.key_manager,
                                                *args,
                                                **kwargs, )
        if res["status"] == 0:
            self.cur_contract_id += 1
        return res

    def show_contract(self, contract_id):
        """
        Display the content of a contract.

        Parameters:
            contract_id: id of the contract that the caller wants to see

        Returns:
        An object with the following fields:
            a_dest: a list of ids of the destination agents
            de: a list of ids of the data elements
            template: which template function
            args: arguments to the template function
            kwargs: kwargs to the template function
        """
        return contract_manager.show_contract(self.caller_id, contract_id)

    def show_my_contracts_pending_approval(self):
        """
        Display all contracts that the caller is a destination agent for, and has not been approved.
        """
        return contract_manager.show_my_contracts_pending_approval(self.caller_id)

    def show_contracts_pending_my_approval(self):
        """
        Display all contracts that the caller has not approved yet.
        """
        return contract_manager.show_contracts_pending_my_approval(self.caller_id)

    def approve_contract(self, contract_id):
        """
        Update a contract's status to approved (1), for source agent <user_id>.

        Parameters:
            contract_id: id of the contract

        Returns:
        A response object with the following fields:
            status: status of approving contract. 0: success, 1: failure.
        """
        return contract_manager.approve_contract(self.caller_id,
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

    def upload_cmp(self, dest_a_id, de_id, function):
        return contract_manager.upload_cmp(self.caller_id,
                                           dest_a_id,
                                           de_id,
                                           function,
                                           self.write_ahead_log,
                                           self.key_manager, )

    def store(self, key, value):
        if self.trust_mode == "no_trust":
            return self.app_state_manager.store(key, value, self.key_manager)
        return self.app_state_manager.store(key, value)

    def load(self, key):
        return self.app_state_manager.load(key)

    # def execute_contract(self, user_id, contract_id):
    #     """
    #     Execute a contract.
    #
    #     Parameters:
    #         user_id: caller id
    #         contract_id: id of the contract
    #
    #     Returns:
    #         The result of executing the contract (f(P))
    #     """
    #
    #     # fetch arguments
    #     function, function_param = contract_manager.get_contract_function_and_param(contract_id)
    #     args = function_param["args"]
    #     kwargs = function_param["kwargs"]
    #
    #     print("Function called in contract is", function)
    #
    #     # Case 1: in development mode, we mimic the behaviour of Gatekeeper
    #     if self.development_mode:
    #         # Check destination agent
    #         dest_a_ids = contract_manager.get_dest_ids_for_contract(contract_id)
    #         if int(user_id) not in dest_a_ids and len(dest_a_ids):
    #             print("Caller not a destination agent")
    #             return None
    #
    #         # Check contract status
    #         contract_ready_flag = contract_manager.check_contract_ready(contract_id)
    #         if not contract_ready_flag:
    #             print("This contract's status is not approved.")
    #             return None
    #
    #         # Get accessible data elements
    #         contract_de_ids = contract_manager.get_de_ids_for_contract(contract_id)
    #         # print(f"Accessible DEs in the contract are: {contract_de_ids}")
    #
    #         # get_des_by_ids_res = database_api.get_des_by_ids(all_accessible_de_id)
    #         # if get_des_by_ids_res["status"] == 1:
    #         #     print("Something wrong with getting accessible DE for contract.")
    #         #     return get_des_by_ids_res
    #         #
    #         # accessible_de = []
    #         # for cur_de in get_des_by_ids_res["data"]:
    #         #     if self.trust_mode == "no_trust":
    #         #         data_owner_symmetric_key = self.key_manager.get_agent_symmetric_key(cur_de.owner_id)
    #         #     else:
    #         #         data_owner_symmetric_key = None
    #         #     cur_de = DataElement(cur_de.id,
    #         #                          cur_de.name,
    #         #                          cur_de.type,
    #         #                          cur_de.access_param,
    #         #                          data_owner_symmetric_key)
    #         #     accessible_de.append(cur_de)
    #         #
    #         # for cur_de in accessible_de:
    #         #     if cur_de.type == "file":
    #         #         cur_de.access_param = os.path.join(self.storage_path, cur_de.access_param)
    #         #
    #         self.accessible_de_development = contract_de_ids
    #         #
    #         # print(self.accessible_de_development)
    #
    #         # Execute function
    #         list_of_function = get_registered_functions()
    #
    #         for cur_fn in list_of_function:
    #             if function == cur_fn.__name__:
    #                 print("Calling a function in development:", function)
    #                 print("Args is", args)
    #                 print("Kwargs is ", kwargs)
    #                 res = cur_fn(*args, **kwargs)
    #                 # Writing the result to caller's staging storage, under file name {contract_id}.
    #                 # self.write_staged(contract_id, user_id, res)
    #                 # print(res)
    #                 # Check: if destination agents list is empty, this is a materialized intermediate DE
    #                 # So we register it first, then write it to SM_storage, with f_name equal to its DE id,
    #                 # owner_id equals to 0
    #                 if not len(dest_a_ids):
    #                     # Decide which de_id to use from self.cur_de_id
    #                     cur_de_id = self.cur_de_id
    #                     self.cur_de_id += 1
    #
    #                     # TODO: this needs to be modified
    #                     de_manager.register_de_in_DB(cur_de_id,
    #                                                  0,  # owner id is 0
    #                                                  contract_id,
    #                                                  "file",
    #                                                  cur_de_id,
    #                                                  self.write_ahead_log,
    #                                                  self.key_manager)
    #                     # Assume register runs sucessfully
    #                     self.write_intermediate_DE(cur_de_id, res)
    #                     return f"Intermediate DE with ID {cur_de_id} created."
    #                 else:
    #                     return res
    #
    #         # Getting here means called function is not found
    #         print("Called function does not exist")
    #         return None
    #     else:
    #         # Case 2: Sending to Gatekeeper
    #         print("Calling a function in docker:", function)
    #         res = self.gatekeeper.call_api(function,
    #                                        user_id,
    #                                        contract_id,
    #                                        *args,
    #                                        **kwargs)
    #         return res

    def call_api(self, agent_token, api, *args, **kwargs):
        """
        Calls an API as the given user

        Parameters:
         agent_token: the authentication token of the agent
         api: api to call
         *args, **kwargs: arguments to the API call

        Returns:
         result of calling the API
        """

        self.caller_id = agent_manager.authenticate_agent(agent_token)
        list_of_api_endpoints = get_registered_api_endpoint()
        list_of_functions = get_registered_functions()

        # For development mode
        if self.development_mode:
            # First see if it's a function
            for cur_f in list_of_functions:
                if api == cur_f.__name__:
                    print(f"Development mode: Calling function: {api}")
                    self.api_type_development = "function"
                    # Step 1: Running the function
                    res = cur_f(*args, **kwargs)

                    # Step 2: Function returns. We check if it can be released by asking contract_manager.
                    # We know the caller, the args, and DEs accessed (in development mode)
                    param_json = {"args": args, "kwargs": kwargs}
                    param_str = json.dumps(param_json)
                    release_status = contract_manager.check_release_status(self.caller_id,
                                                                           self.accessed_de_development,
                                                                           api,
                                                                           param_str)
                    # Clear self.accessed_de_development
                    self.accessed_de_development = set()
                    print(res)
                    print("Release status:", release_status)
                    if release_status:
                        return res
                    else:
                        res = {"status": 1, "message": "Contract result has not been approved to be released."}
                        return res
            # Else see if it's a pure api endpoint
            for cur_api in list_of_api_endpoints:
                if api == cur_api.__name__:
                    self.api_type_development = "api_endpoint"
                    print(f"Development mode: Calling pure api_endpoint: {api}")
                    return cur_api(*args, **kwargs)
            print("Development mode: Called api_endpoint not found:", api)
            return None
        else:
            # First see if it's a function
            for cur_f in list_of_functions:
                if api == cur_f.__name__:
                    gatekeeper_res = self.gatekeeper.call_api(api,
                                                              self.caller_id,
                                                              self.cur_de_id,
                                                              *args,
                                                              **kwargs)
                    derived_des_to_create = gatekeeper_res["derived_des_to_create"]
                    de_ids_accessed = gatekeeper_res["de_ids_accessed"]
                    # Check to see if this function call creates any derived DEs. If yes, register and store them now.
                    if len(derived_des_to_create) > 0:
                        for derived_de in derived_des_to_create:
                            print("Source DE for this derived DE is", de_ids_accessed)
                            res = self.register_de(self.caller_id, derived_de[1], True, derived_de[0])
                            if res["status"]:
                                return res
                            for src_de_id in de_ids_accessed:
                                if self.write_ahead_log:
                                    wal_entry = f"database_api.create_derived_de({res['de_id']}, {src_de_id})"
                                    self.write_ahead_log.log(self.caller_id, wal_entry, self.key_manager, )
                                db_res = database_api.create_derived_de(res["de_id"], src_de_id)
                                if db_res["status"] == 1:
                                    return db_res
                            # Encrypt with DS sym key if needed
                            pickled_bytes = pickle.dumps(derived_de[2])
                            if self.trust_mode == "no_trust":
                                pickled_bytes = cu.encrypt_data_with_symmetric_key(pickled_bytes,
                                                                                   self.key_manager.agents_symmetric_key[
                                                                                       0])
                            self.storage_manager.write(res["de_id"], pickled_bytes, "object")
                    if gatekeeper_res["status"] == 0:
                        return gatekeeper_res["result"]
                    return None
            # Else see if it's a pure api endpoint
            for cur_api in list_of_api_endpoints:
                if api == cur_api.__name__:
                    print(f"Calling pure api_endpoint: {api}")
                    return cur_api(*args, **kwargs)
            print("Called api_endpoint not found:", api)
            return None

        # # First we check if we are in development mode, if true, call call_api_development
        # if self.development_mode:
        #     res = self.call_api_development(api, *args, **kwargs)
        #     return res
        #     if api == cur_api.__name__:
        #         print("user is calling an api_endpoint", api)
        #         # print(args)
        #         res = cur_api(self.caller_id, *args, **kwargs)
        #         return res
        #
        # print("Called api_endpoint not found:", api)
        # return None

    def write_intermediate_DE(self, de_id, content):
        dir_path = os.path.join(self.storage_path, str(de_id))
        dst_file_path = os.path.join(dir_path, str(de_id))
        os.makedirs(dir_path, exist_ok=True)
        with open(dst_file_path, 'wb+') as f:
            f.write(pickle.dumps(content))
        with open(dst_file_path, "rb") as f:
            cur_content = pickle.load(f)
            print(cur_content)

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

    # def call_api_development(self, api, *args, **kwargs):
    #     """
    #     For testing: handling api calls in development mode
    #     """
    #
    #     # Check if api called is valid (either a function or an api_endpoint)
    #     api_type = None
    #     list_of_api_endpoint = get_registered_api_endpoint()
    #     # print(api)
    #     for cur_api in list_of_api_endpoint:
    #         # print(cur_api.__name__)
    #         if api == cur_api.__name__:
    #             api_type = "api_endpoint"
    #             break
    #
    #     if api_type is None:
    #         print("Called API endpoint not found in EPM:", api)
    #         return None
    #
    #     # Case 1: calling non-jail function
    #     for cur_api in list_of_api_endpoint:
    #         if api == cur_api.__name__:
    #             print(f"Calling api_endpoint in dev mode: {api}")
    #             res = cur_api(*args, **kwargs)
    #             return res

    # def get_contract_de_ids(self):
    #     """
    #     For testing: when in development mode, fetches all DEs.
    #
    #     Parameters:
    #
    #     Returns:
    #         a list of DE ids for the current contract
    #     """
    #
    #     if not self.development_mode:
    #         return -1
    #
    #     return self.accessible_de_development

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
