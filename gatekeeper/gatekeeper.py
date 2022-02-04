import os
import pathlib
import socket
import subprocess
import time
from multiprocessing import Process

from dsapplicationregistration.dsar_core import (register_connectors,
                                                 get_names_registered_functions,
                                                 get_registered_functions,
                                                 get_registered_dependencies,)
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
                                              to_api=cur_to_api,)
            database_service_response = database_api.create_api_dependency(api_dependency_db)
            if database_service_response.status == -1:
                return Response(status=1, message="internal database error")
    return Response(status=0, message="Gatekeeper setup success")


def get_accessible_data(user_id, api):
    policy_info = policy_broker.get_user_api_info(user_id, api)
    return policy_info


def call_api(api, cur_username, *args, **kwargs):

    # TODO: add the intent-policy matching process in here

    # get current user id
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username,))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    # look at the accessible data by policy for current (user, api)
    policy_info = get_accessible_data(cur_user_id, api)
    accessible_data_policy = policy_info.accessible_data

    # look at all optimistic data from the DB
    optimistic_data= database_api.get_all_optimistic_datasets()
    accessible_data_optimistic = []
    for i in range(len(optimistic_data.data)):
        cur_optimistic_id = optimistic_data.data[i].id
        accessible_data_optimistic.append(cur_optimistic_id)

    # Combine these two types of accessible data elements together
    all_accessible_data_id = set(accessible_data_policy + accessible_data_optimistic)
    print("all accessible data elements are: ")
    print(all_accessible_data_id)


    # zz: mount data station's storage dir to mount point that encodes user_id and api name using interceptor
    # zz: run api
    # zz: record all data ids that are accessed by the api through interceptor
    # zz: check whether access to those data ids is valid, if not we cannot release the results

    # global data_ids_accessed
    # data_ids_accessed = set()


    ds_config = utils.parse_config("data_station_config.yaml")
    ds_storage_path = pathlib.Path(ds_config["storage_path"]).absolute()
    ds_storage_mount_path = ds_config["mount_path"]

    mount_point = pathlib.Path(os.path.join(ds_storage_mount_path, str(cur_user_id), api)).absolute()
    pathlib.Path(mount_point).mkdir(parents=True, exist_ok=True)

    interceptor_path = pathlib.Path(ds_config["interceptor_path"]).absolute()
    # print(interceptor_path, ds_storage_path, mount_point)

    subprocess.call(["python", str(interceptor_path), str(ds_storage_path), str(mount_point)], shell=False)
    # time.sleep(5)

    # interceptor_process = Process(target=interceptor.main, args=(ds_storage_path, mount_point))
    # interceptor_process.start()
    # os.system("umount " + mount_point)
    # interceptor_process.join()

    # Getting these data elements from the DB
    # all_accessible_data = filter(lambda data: data.id in all_accessible_data_id,
    #                              database_api.get_all_datasets().data)
    # print("looking at the access types:")
    # for cur_data in all_accessible_data:
    #     print(cur_data.access_type)

    # Acutally calling the api
    status = "Error"
    list_of_apis = get_registered_functions()
    for cur_api in list_of_apis:
        if api == cur_api.__name__:
            status =  cur_api(*args, **kwargs)


    os.system("umount " + str(mount_point))

    # host = "localhost"
    # port = 6666
    # sock = socket.socket()
    # sock.connect((host, port))
    # while True:
    #     data = sock.recv(1024)
    #     if not data:
    #         break
    #     print(data.decode())
    # sock.close()

    time.sleep(1)
    print("Data ids accessed:")
    with open("/tmp/data_ids_accessed.txt", 'r') as f:
        print(f.read())
    os.remove("/tmp/data_ids_accessed.txt")

    return status

def record_data_ids_accessed(data_path, user_id, api_name):
    data_id = database_api.get_dataset_by_access_type(data_path).data[0].id
    # data_id = 666
    # data_ids_accessed.add(data_id)
    # ds_config = utils.parse_config("data_station_config.yaml")
    # ds_storage_path = pathlib.Path(ds_config["storage_path"]).absolute()
    # f_path = os.path.join(str(ds_storage_path), "data_ids_accessed.txt")
    # f = open("/tmp/data_ids_accessed.txt", 'a+')
    # f.write(str(data_id) + '\n')
    # f.close()
    return data_id
