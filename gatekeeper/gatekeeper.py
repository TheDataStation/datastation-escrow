import os
import pathlib
import time
import multiprocessing

from Interceptor import interceptor
from dsapplicationregistration.dsar_core import (register_connectors,
                                                 get_names_registered_functions,
                                                 get_registered_functions,
                                                 get_registered_dependencies, )
from dbservice import database_api
from policybroker import policy_broker
from models.api import *
from models.api_dependency import *
from models.user import *
from models.response import *
from common import utils


def gatekeeper_setup(connector_name, connector_module_path):
    # print("Start setting up the gatekeeper")
    register_connectors(connector_name, connector_module_path)
    # print("Check registration results:")
    # TODO: change this part once the interceptor has been added in
    # add dependency X -> "dir_accessible" if X does not depend on anything else
    apis_to_register = get_names_registered_functions()
    dependencies_to_register = get_registered_dependencies()
    for cur_api in apis_to_register:
        if cur_api not in dependencies_to_register.keys():
            dependencies_to_register[cur_api] = ["dir_accessible"]
    # add "dir_accessible" to list of apis
    if "dir_accessible" not in apis_to_register:
        apis_to_register.append("dir_accessible")
    # print(apis_to_register)
    # print(dependencies_to_register)

    # now we call dbservice to register these info in the DB
    for cur_api in apis_to_register:
        api_db = API(api_name=cur_api)
        database_service_response = database_api.create_api(api_db)
        if database_service_response.status == -1:
            return Response(status=1, message="internal database error")
    for cur_from_api in dependencies_to_register:
        to_api_list = dependencies_to_register[cur_from_api]
        for cur_to_api in to_api_list:
            api_dependency_db = APIDependency(from_api=cur_from_api,
                                              to_api=cur_to_api, )
            database_service_response = database_api.create_api_dependency(api_dependency_db)
            if database_service_response.status == -1:
                return Response(status=1, message="internal database error")
    return Response(status=0, message="Gatekeeper setup success")


def get_accessible_data(user_id, api):
    policy_info = policy_broker.get_user_api_info(user_id, api)
    return policy_info


def foo(a, b):
    print("in foo")


def test_api():
    # print(files)
    # for file in files:
    #     # print(file)
    #     if pathlib.Path(file).is_file():
    #         with open(file, "r") as cur_file:
    #             cur_file.readline()
    #         print("read ", file)
    import glob
    ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    mount_path = pathlib.Path(ds_config["mount_path"]).absolute()
    files = glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True)
    # print(set(files))
    for file in set(files):
        # print(file)
        if pathlib.Path(file).is_file():
            with open(file, "r") as cur_file:
                cur_file.readline()
            print("read ", file)


def call_actual_api(api_name, connector_name, connector_module_path,
                    accessible_data_dict, accessible_data_paths, api_conn, *args, **kwargs):
    api_pid = os.getpid()
    print("api process id:", str(api_pid))
    accessible_data_dict[api_pid] = accessible_data_paths

    # print("xxxxxxxxxx")
    # print(api_name, *args, **kwargs)
    register_connectors(connector_name, connector_module_path)
    list_of_apis = get_registered_functions()
    # print("list_of_apis:", list_of_apis)
    for cur_api in list_of_apis:
        if api_name == cur_api.__name__:
            # print("call", api_name)
            result = cur_api(*args, **kwargs)
            api_conn.send(result)


def call_api(api, cur_username, exec_mode, data_station_log, accessible_data_dict, data_accessed_dict, *args, **kwargs):
    # import glob
    # ds_path = str(pathlib.Path(os.path.dirname(os.path.abspath(__file__))).parent)
    # ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))
    # mount_path = pathlib.Path(ds_config["storage_path"]).absolute()
    # all_files = set(glob.glob(os.path.join(str(mount_path), "**/**/**/*"), recursive=True))
    # all_files_mounted = set()
    # for f in all_files:
    #     all_files_mounted.add(f.replace("SM_storage", "SM_storage_mount"))

    # TODO: add the intent-policy matching process in here

    # get current user id
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        print("Something wrong with the current user")
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    # look at the accessible data by policy for current (user, api)
    policy_info = get_accessible_data(cur_user_id, api)
    accessible_data_policy = policy_info.accessible_data

    # look at all optimistic data from the DB
    optimistic_data = database_api.get_all_optimistic_datasets()
    accessible_data_optimistic = []
    for i in range(len(optimistic_data.data)):
        cur_optimistic_id = optimistic_data.data[i].id
        accessible_data_optimistic.append(cur_optimistic_id)

    # Combine these two types of accessible data elements together
    if exec_mode == "optimistic":
        all_accessible_data_id = set(accessible_data_policy + accessible_data_optimistic)
    else:
        all_accessible_data_id = set(accessible_data_policy)
    print("all accessible data elements are: ")
    print(all_accessible_data_id)

    # TODO: change this once definite vs. indefinite can be determined
    #       Question: how do we know if an intent is definite or indefinite?
    #       Right now we assume that all intents are indefinite intents

    accessible_data_paths = set()
    for cur_id in all_accessible_data_id:
        accessible_data_paths.add(str(database_api.get_dataset_by_id(cur_id).data[0].access_type))

    # Actually calling the api
    print("current process id:", str(os.getpid()))

    app_config = utils.parse_config("app_connector_config.yaml")
    connector_name = app_config["connector_name"]
    connector_module_path = app_config["connector_module_path"]

    main_conn, api_conn = multiprocessing.Pipe()
    api_process = multiprocessing.Process(target=call_actual_api,
                                          args=(api, connector_name, connector_module_path,
                                                accessible_data_dict, accessible_data_paths, api_conn,
                                                *args),
                                          kwargs=kwargs)
    api_process.start()
    api_pid = api_process.pid
    # print("api process id:", str(api_pid))
    # signal.set()
    # accessible_data_dict[api_pid] = accessible_data_paths
    api_process.join()
    api_result = main_conn.recv()
    # signal.clear()

    if api_pid in accessible_data_dict.keys():
        del accessible_data_dict[api_pid]

    data_ids_accessed = set()
    if api_pid in data_accessed_dict.keys():
        cur_data_accessed = data_accessed_dict[api_pid].copy()
        del data_accessed_dict[api_pid]

        for path in cur_data_accessed:
            data_id = record_data_ids_accessed(path, cur_user_id, api)
            if data_id != None:
                data_ids_accessed.add(data_id)
            else:
                # os.remove("/tmp/data_accessed.txt")
                return Response(status=1, message="cannot get data id from data path")

    print("Data ids accessed:")
    print(data_ids_accessed)

    if set(data_ids_accessed).issubset(set(accessible_data_policy)):
        print("All data access allowed by policy.")
        # log operation: logging intent_policy match
        data_station_log.log_intent_policy_match(cur_user_id, api, data_ids_accessed)
        print("api_result: ", api_result)
        return api_result
    elif set(data_ids_accessed).issubset(all_accessible_data_id):
        print("Some access to optimistic data not allowed by policy.")
        # log operation: logging intent_policy mismatch
        data_station_log.log_intent_policy_mismatch(cur_user_id, api, data_ids_accessed, set(accessible_data_policy))
        return Response(status=1, message="Some access to optimistic data not allowed by policy.")
    else:
        # We should not get in here in the first place.
        # TODO: illegal access can still happen since interceptor does not block access
        #  (except filter out inaccessible data when list dir)
        print("Access to illegal data happened. Something went wrong")
        data_station_log.log_intent_policy_mismatch(cur_user_id, api, data_ids_accessed, set(accessible_data_policy))
        return Response(status=1, message="Access to illegal data happened. Something went wrong.")


def record_data_ids_accessed(data_path, user_id, api_name):
    response = database_api.get_dataset_by_access_type(data_path)
    if response.status != 1:
        print("get_dataset_by_access_type database error")
        return None
    else:
        data_id = response.data[0].id
        return data_id


if __name__ == '__main__':
    call_api("preprocess", "xxx")
