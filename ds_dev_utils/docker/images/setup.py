import time
import socket
import pickle
import os
import requests

from run_function import (load_connectors,
                            run_function)
# from Interceptor import interceptor
# import multiprocessing
from dsapplicationregistration.dsar_core import get_registered_functions

def main():
    # time.sleep(3)
    # load connectors before doing anything
    connector_dir = "/usr/src/ds/functions"
    load_connectors(connector_dir)
    print("setting up...")

    # # Setting up the interceptor
    #
    # manager = multiprocessing.Manager()
    #
    # storage_path = "/mnt/data"
    # mount_path = "/mnt/data_mount"
    #
    # interceptor_process = multiprocessing.Process(target=interceptor.main,
    #                                               args=(storage_path,
    #                                                     mount_path,
    #                                                     {},
    #                                                     {}))
    #
    # interceptor_process.start()
    #
    # print("starting interceptor...")
    # counter = 0
    # while not os.path.ismount(mount_path):
    #     time.sleep(1)
    #     counter += 1
    #     if counter == 10:
    #         print("mount time out")
    #         exit(1)
    # print("Mounted {} to {}".format(storage_path, mount_path))
    # print(os.path.dirname(os.path.realpath(__file__)))

    # notice that communication from container to host doesn't
    #  need a port mapping

    # send signal that the container has started, and keep trying until it succeeds
    #  stack overflow: https://stackoverflow.com/questions/27062099/python-requests-retrying-until-a-valid-response-is-received
    cnt = 0
    max_retry = 3
    while cnt < max_retry:
        try:
            response = requests.get('http://host.docker.internal:3030/started')
            if response.status_code == requests.codes.ok:
                break
            else:
                raise RuntimeError(
                        "requests.get: wrong status code")
        except requests.exceptions.RequestException as e:
            time.sleep(2**cnt)
            cnt += 1
            if cnt >= max_retry:
                raise e

    print(response.content)

    # request function to run
    response = requests.get('http://host.docker.internal:3030/function')
    function_dict = pickle.loads(response.content)
    print("function dictionary: ", function_dict)

    # run the function and pickle it
    ret = run_function(function_dict["function"], *function_dict["args"], **function_dict["kwargs"])
    to_send_back = pickle.dumps({"return_value":ret})

    print(to_send_back)

    # send the return value of the function
    response = requests.post("http://host.docker.internal:3030/function_return",
            data=to_send_back,
            # headers={'Content-Type': 'application/octet-stream'},
            )
    print(response, response.content)

    time.sleep(1000000)
if __name__ ==  '__main__':
    main()