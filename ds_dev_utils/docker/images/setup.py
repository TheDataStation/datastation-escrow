import sys
import time
import socket
import pickle
import os
import requests
from ast import literal_eval

from run_function import (load_connectors, run_function)
from Interceptor import interceptor
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

    accessible_data_obj = config_dict["accessible_data_dict"]
    docker_id = config_dict["docker_id"]

    connector_dir = "/usr/src/ds/functions"
    load_connectors(connector_dir)
    print("setting up...")

    # Setting up the interceptor
    manager = multiprocessing.Manager()

    accessible_data_dict = manager.dict()
    data_accessed_dict = manager.dict()

    accessible_data_dict[main_pid] = accessible_data_obj

    storage_path = "/mnt/data"
    mount_path = "/mnt/data_mount"

    interceptor_process = multiprocessing.Process(target=interceptor.main,
                                                  args=(storage_path,
                                                        mount_path,
                                                        accessible_data_dict,
                                                        data_accessed_dict))

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
            response = requests.get('http://host.docker.internal:3030/started', params=send_params)
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

    # request function to run
    response = requests.get('http://host.docker.internal:3030/function', params=send_params)
    function_dict = pickle.loads(response.content)
    print("function dictionary: ", function_dict)

    # run the function and pickle it
    ret = run_function(function_dict["function"], *function_dict["args"], **function_dict["kwargs"])
    print("Return value is", ret)
    print(dict(data_accessed_dict))

    data_accessed = dict(data_accessed_dict)[main_pid]
    to_send_back = pickle.dumps({"return_value": ret, "data_accessed": data_accessed})

    # Before we return the result of the function, look at the data elements accessed
    print("In setup.py: data accessed is", data_accessed_dict)
    # print(to_send_back)

    # send the return value of the function
    response = requests.post("http://host.docker.internal:3030/function_return",
                             data=to_send_back,
                             params=send_params,
                             # headers={'Content-Type': 'application/octet-stream'},
                             )
    print(response, response.content)

    time.sleep(1000000)


if __name__ == '__main__':
    main()
