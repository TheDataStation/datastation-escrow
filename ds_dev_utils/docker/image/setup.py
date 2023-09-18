import sys
import time
import socket
import pickle
import os
import requests
from ast import literal_eval

from run_function import (load_connectors, run_function)
from Interceptor import interceptor
from escrow_api_docker import EscrowAPIDocker
from escrowapi.escrow_api import EscrowAPI
import multiprocessing
from dsapplicationregistration.dsar_core import get_registered_functions


def main():
    # time.sleep(3)
    # load connectors before doing anything

    # Get the PID of the current process
    main_pid = os.getpid()
    print("setup.py: main PID is", main_pid)

    with open("/usr/src/ds/args.pkl", "rb") as f:
        config_dict_data_bytes = f.read()
        config_dict = pickle.loads(config_dict_data_bytes)

    print(config_dict)

    accessible_de = config_dict["accessible_de"]
    docker_id = config_dict["docker_id"]
    agents_symmetric_key = config_dict["agents_symmetric_key"]

    connector_dir = "/usr/src/ds/functions"
    load_connectors(connector_dir)
    print("setting up...")

    # Set up escrow_api docker
    escrow_api_docker = EscrowAPIDocker(accessible_de, agents_symmetric_key)
    EscrowAPI.set_comp(escrow_api_docker)

    # Set up the file interceptor using info from accessible_de

    accessible_data_set = set()
    accessible_data_key_dict = {}
    for cur_de in accessible_de:
        if cur_de.type == "file":
            cur_data_path = os.path.join("/mnt/data", str(cur_de.id), cur_de.name)
            accessible_data_set.add(cur_data_path)
            accessible_data_key_dict[cur_data_path] = cur_de.enc_key

    accessible_data_obj = (accessible_data_set, accessible_data_key_dict)

    manager = multiprocessing.Manager()

    accessible_data_dict = manager.dict()
    data_accessed_dict = manager.dict()
    decryption_time_dict = manager.dict()

    accessible_data_dict[main_pid] = accessible_data_obj

    storage_path = "/mnt/data"
    mount_path = "/mnt/data_mount"

    interceptor_process = multiprocessing.Process(target=interceptor.main,
                                                  args=(storage_path,
                                                        mount_path,
                                                        accessible_data_dict,
                                                        data_accessed_dict,
                                                        decryption_time_dict,))

    interceptor_process.start()

    print("starting interceptor...")
    counter = 0
    while not os.path.ismount(mount_path):
        time.sleep(1)
        counter += 1
        if counter == 10:
            print("mount time out")
            exit(1)
    print("Mounted {} to {}".format(storage_path, mount_path))
    print(os.path.dirname(os.path.realpath(__file__)))

    send_params = {"docker_id": docker_id}

    # notice that communication from container to host doesn't
    #  need a port mapping

    # send signal that the container has started, and keep trying until it succeeds
    #  stack overflow: https://stackoverflow.com/questions/27062099/python-requests-retrying-until-a-valid-response-is-received
    cnt = 0
    max_retry = 3
    while cnt < max_retry:
        try:
            response = requests.get('http://127.0.0.1:3030/started', params=send_params)
            if response.status_code == requests.codes.ok:
                break
            else:
                raise RuntimeError(
                    "requests.get: wrong status code")
        except requests.exceptions.RequestException as e:
            time.sleep(2 ** cnt)
            cnt += 1
            if cnt >= max_retry:
                raise e

    print(response.content)

    # print("Start counting time......")
    # start = time.time()

    # request function to run
    response = requests.get('http://127.0.0.1:3030/get_function_dict', params=send_params)
    function_dict = pickle.loads(response.content)
    # print("function dictionary: ", function_dict)
    # print(time.time() - start)
    # start = time.time()

    # run the function and pickle it
    ret = run_function(function_dict["function"], *function_dict["args"], **function_dict["kwargs"])
    # print("Return value is", ret)
    # print(dict(data_accessed_dict))
    # print(dict(decryption_time_dict))
    # print("Got function return......")
    # print(time.time() - start)
    # print(f"Size of returned value is {sys.getsizeof(ret)}")
    start = time.time()

    if main_pid in dict(data_accessed_dict):
        data_accessed = dict(data_accessed_dict)[main_pid]
    else:
        data_accessed = []
    decryption_time = dict(decryption_time_dict)["total_time"]

    print(data_accessed)

    to_send_back = pickle.dumps({"return_value": ret,
                                 "data_accessed": data_accessed,
                                 "decryption_time": decryption_time})
    # print("To send back pickle constructed......")
    # print(time.time() - start)
    # start = time.time()

    # Before we return the result of the function, look at the data elements accessed
    # print("In setup.py: data accessed is", data_accessed_dict)
    # print(to_send_back)

    # send the return value of the function
    response = requests.post("http://127.0.0.1:3030/send_function_return",
                             data=to_send_back,
                             params=send_params,
                             )
    # print(response, response.content)
    # print("Send function return finished......")
    # print(time.time() - start)


if __name__ == '__main__':
    main()
