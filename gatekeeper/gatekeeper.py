import os
import pathlib
import time
import multiprocessing
import pathlib

from dsapplicationregistration.dsar_core import (register_connectors,
                                                 get_names_registered_functions,
                                                 get_registered_functions,
                                                 get_registered_dependencies, )
from dbservice import database_api
from policybroker import policy_broker
from common.pydantic_models.api import API
from common.pydantic_models.api_dependency import APIDependency
from common.pydantic_models.user import User
from common.pydantic_models.response import Response, APIExecResponse
from common import general_utils
from crypto import cryptoutils as cu
from ds_dev_utils import jail_utils

from verifiability.log import Log
from writeaheadlog.write_ahead_log import WAL
from crypto.key_manager import KeyManager
from ds_dev_utils.jail_utils import DSDocker


class Gatekeeper:
    def __init__(self,
                 data_station_log: Log,
                 write_ahead_log: WAL,
                 key_manager: KeyManager,
                 trust_mode: str,
                 accessible_data_dict,
                 data_accessed_dict,
                 connector_name,
                 connector_module_path,
                 mount_dir,
                 ):
        """
        The general class for the gatekeeper, which brokers access to data.
        """

        # save variables
        self.data_station_log = data_station_log
        self.write_ahead_log = write_ahead_log
        self.key_manager = key_manager
        self.trust_mode = trust_mode

        self.accessible_data_dict = accessible_data_dict
        self.data_accessed_dict = data_accessed_dict
        self.connector_name = connector_name
        self.connector_module_path = connector_module_path
        self.mount_dir = mount_dir

        # print("Start setting up the gatekeeper")
        register_connectors(connector_name, connector_module_path)
        # print("Check registration results:")
        apis_to_register = get_names_registered_functions()
        dependencies_to_register = get_registered_dependencies()
        # print(dependencies_to_register)

        # now we call dbservice to register these info in the DB
        for cur_api in apis_to_register:
            api_db = API(api_name=cur_api)
            database_service_response = database_api.create_api(api_db)
            if database_service_response.status == -1:
                print("database_api.create_api: internal database error")
                raise RuntimeError(
                    "database_api.create_api: internal database error")
        for cur_from_api in dependencies_to_register:
            to_api_list = dependencies_to_register[cur_from_api]
            for cur_to_api in to_api_list:
                api_dependency_db = APIDependency(from_api=cur_from_api,
                                                  to_api=cur_to_api, )
                database_service_response = database_api.create_api_dependency(
                    api_dependency_db)
                if database_service_response.status == -1:
                    print("database_api.create_api_dependency: internal database error")
                    raise RuntimeError(
                        "database_api.create_api_dependency: internal database error")
        print("Gatekeeper setup success")

    def get_accessible_data(self, user_id, api):
        accessible_data = policy_broker.get_user_api_info(user_id, api)
        return accessible_data

    # We add times to the following function to record the overheads

    def call_api(self,
                 api,
                 cur_user_id,
                 exec_mode,
                 *args,
                 **kwargs):
        """
        Calls the API specified, ensuring that
          - data is only exposed to the API if permitted
          - data accessed by API is allowed

        Parameters:
         api: api to call
         cur_user_id: the user id to decide what data is exposed
         exec_mode: optimistic or pessimistic

        Returns:
         Response based on what happens
        """

        # print(trust_mode)

        # We first determine whether this is a data-blind function or data-aware function
        # data-aware function requires an argument called DE_id

        data_aware_flag = False
        data_aware_DE = set()

        if kwargs.__contains__("DE_id"):
            data_aware_flag = True
            data_aware_DE = set(kwargs["DE_id"])

        # print(data_aware_flag)
        # print(data_aware_DE)

        # look at the accessible data by policy for current (user, api)
        # print(cur_user_id, api)
        accessible_data_policy = self.get_accessible_data(cur_user_id, api)

        # Note: In data-aware-functions, if accessible_data_policy does not include data_aware_DE,
        # we end the execution immediately
        if not data_aware_DE.issubset(set(accessible_data_policy)):
            err_msg = "Attempted to access data not allowed by policies. Execution stops."
            print(err_msg)
            return Response(status=1, message=err_msg)

        # look at all optimistic data from the DB
        optimistic_data = database_api.get_all_optimistic_datasets()
        accessible_data_optimistic = []
        for i in range(len(optimistic_data.data)):
            cur_optimistic_id = optimistic_data.data[i].id
            accessible_data_optimistic.append(cur_optimistic_id)

        # Combine these two types of accessible data elements together
        # In optimistic execution mode, we include optimistic datasets as well
        if exec_mode == "optimistic":
            all_accessible_data_id = set(
                accessible_data_policy + accessible_data_optimistic)
        # In pessimistic execution mode, we only include data that are allowed by policies
        elif not data_aware_flag:
            all_accessible_data_id = set(accessible_data_policy)
        # Lastly, in data-aware execution, all_accessible_data_id should be data_aware_DE
        else:
            all_accessible_data_id = data_aware_DE
        # print("all accessible data elements are: ")
        # print(all_accessible_data_id)

        get_datasets_by_ids_res = database_api.get_datasets_by_ids(
            all_accessible_data_id)
        if get_datasets_by_ids_res.status == -1:
            err_msg = "No accessible data for " + api
            print(err_msg)
            return Response(status=1, message=err_msg)
        accessible_data_paths = set(
            [dataset.access_type for dataset in get_datasets_by_ids_res.data])

        # if in zero trust mode, send user's symmetric key to interceptor in order to decrypt files
        trust_mode = self.trust_mode

        accessible_data_key_dict = None
        if trust_mode == "no_trust":
            # get the symmetric key of each accessible data's owner,
            # and store them in dict to pass to interceptor
            accessible_data_key_dict = {}
            for dataset in get_datasets_by_ids_res.data:
                data_owner_symmetric_key = self.key_manager.get_agent_symmetric_key(
                    dataset.owner_id)
                accessible_data_key_dict[dataset.access_type] = data_owner_symmetric_key

        self.accessible_data_dict[63452] = (accessible_data_paths, accessible_data_key_dict)

        # start a new process for the api call
        main_conn, api_conn = multiprocessing.Pipe()
        api_process = multiprocessing.Process(target=call_actual_api,
                                              args=(api,
                                                    self.connector_name,
                                                    self.connector_module_path,
                                                    self.mount_dir,
                                                    api_conn,
                                                    *args,
                                                    ),
                                              kwargs=kwargs)
        api_process.start()
        # api_pid = api_process.pid
        api_pid = 63452

        # print("api process id:", str(api_pid))
        api_result = main_conn.recv()
        api_process.join()

        # clean up the two dictionaries used for communication,
        # and get the data ids accessed from the list of data paths accessed through the interceptor
        if api_pid in self.accessible_data_dict.keys():
            del self.accessible_data_dict[api_pid]

        # Get the IDs of the DEs that are actually accessed
        data_ids_accessed = set()
        if api_pid in self.data_accessed_dict.keys():
            cur_data_accessed = self.data_accessed_dict[api_pid].copy()
            del self.data_accessed_dict[api_pid]
            get_datasets_by_paths_res = database_api.get_datasets_by_paths(
                cur_data_accessed)
            if get_datasets_by_paths_res.status == -1:
                err_msg = "No accessible data for " + api
                print(err_msg)
                return Response(status=1, message=err_msg)
            data_ids_accessed = set(
                [dataset.id for dataset in get_datasets_by_paths_res.data])

        # print("data id accessed are:")
        # print(data_ids_accessed)

        if set(data_ids_accessed).issubset(set(accessible_data_policy)):
            # print("All data access allowed by policy.")
            # log operation: logging intent_policy match
            self.data_station_log.log_intent_policy_match(cur_user_id,
                                                          api,
                                                          data_ids_accessed,
                                                          self.key_manager,)
            # In this case, we can return the result to caller.
            response = APIExecResponse(status=0,
                                       message="API result can be released",
                                       result=api_result,
                                       )
        elif set(data_ids_accessed).issubset(all_accessible_data_id):
            # print("Some access to optimistic data not allowed by policy.")
            # log operation: logging intent_policy mismatch
            self.data_station_log.log_intent_policy_mismatch(cur_user_id,
                                                             api,
                                                             data_ids_accessed,
                                                             set(accessible_data_policy),
                                                             self.key_manager,)
            response = APIExecResponse(status=-1,
                                       message="Some access to optimistic data not allowed by policy.",
                                       result=[api_result, data_ids_accessed],)
        else:
            # TODO: illegal access can still happen since interceptor does not block access
            #  (except filter out inaccessible data when list dir)
            # print("Access to illegal data happened. Something went wrong")
            # log operation: logging intent_policy mismatch
            self.data_station_log.log_intent_policy_mismatch(cur_user_id,
                                                             api,
                                                             data_ids_accessed,
                                                             set(accessible_data_policy),
                                                             self.key_manager,)
            response = Response(
                status=1, message="Access to illegal data happened. Something went wrong.")

        return response

def call_actual_api(api_name,
                    connector_name,
                    connector_module_path,
                    mount_dir,
                    api_conn,
                    *args,
                    **kwargs,
                    ):
    """
    The thread that runs the API on the Docker container
    TODO: do not hard code API pid

    Parameters:
     api_name: name of API to run on Docker container
     connector_name: name of connector
     connector_module_path: path to connector module
     accessible_data_dict: dictionary of data that API is allowed to access, fed to Interceptor
     accessible_data_paths: paths associated with data dict
     accessible_data_key_dict:
     mount_dir: directory of filesystem mount for Interceptor
     api_conn: variables to be passed from parent to child thread, including API result

    Returns:
     None
    """

    print(os.path.dirname(os.path.realpath(__file__)))
    # print(api_name, *args, **kwargs)
    register_connectors(connector_name, connector_module_path)
    list_of_apis = get_registered_functions()
    # print("list_of_apis:", list_of_apis)
    time.sleep(1)
    print("connector name / module path: ", connector_name, connector_module_path)
    print("accessed path: " + os.path.dirname(os.path.realpath(__file__)) + "/../" + connector_module_path,)
    session = DSDocker(
        os.path.dirname(os.path.realpath(__file__)) + "/../" + connector_module_path,
        "connector_file",
        mount_dir,
        os.path.dirname(os.path.realpath(__file__)) + "/../" + "ds_dev_utils/docker/images",
    )

    # print(session.container.top())

    # run function
    # session.direct_run("read_file", "/mnt/data/hi.txt")
    for cur_api in list_of_apis:
        if api_name == cur_api.__name__:
            print("call", api_name)
            ret = session.flask_run(api_name, *args, **kwargs)

            # result = cur_api(*args, **kwargs)
            api_conn.send(ret)
            api_conn.close()
            break

    # clean up
    session.stop_and_prune()


def call_actual_api_(api_name,
                     connector_name,
                     connector_module_path,
                     accessible_data_dict,
                     accessible_data_paths,
                     accessible_data_key_dict,
                     api_conn,
                     *args,
                     **kwargs,
                     ):

    api_pid = os.getpid()
    # print("api process id:", str(api_pid))
    # set the list of accessible data for this api call,
    # and the corresponding data owner's symmetric keys if running in no trust mode
    accessible_data_dict[api_pid] = (
        accessible_data_paths, accessible_data_key_dict)

    print(os.path.dirname(os.path.realpath(__file__)))
    # print(api_name, *args, **kwargs)
    register_connectors(connector_name, connector_module_path)
    list_of_apis = get_registered_functions()
    # print("list_of_apis:", list_of_apis)
    for cur_api in list_of_apis:
        if api_name == cur_api.__name__:
            # print("call", api_name)
            result = cur_api(*args, **kwargs)
            api_conn.send(result)
            api_conn.close()
            break

# We add times to the following function to record the overheads

if __name__ == '__main__':
    print("Gatekeeper starting.")
