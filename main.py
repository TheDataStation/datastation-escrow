import os
import sys
from storagemanager.storage_manager import StorageManager
from verifiability.log import Log
from crypto.key_manager import KeyManager
from gatekeeper import gatekeeper
from clientapi.client_api import ClientAPI
from common.utils import parse_config

def initialize_system(ds_config, app_config):

    # print(ds_config)
    # print(app_config)

    # In this function we set up all components that need to be initialized

    # set up an instance of the storage_manager
    storage_path = ds_config["storage_path"]
    storage_manager = StorageManager(storage_path)

    # set up an instance of the log
    log_in_memory_flag = ds_config["log_in_memory"]
    data_station_log = Log(log_in_memory_flag)

    # set up an instance of the key manager
    key_manager = KeyManager()

    # get the trust mode for the data station
    trust_mode = ds_config["trust_mode"]

    # lastly, set up an instance of the client_api
    client_api = ClientAPI(storage_manager,
                           data_station_log,
                           key_manager,
                           trust_mode,)

    # set up the application registration in the gatekeeper
    connector_name = app_config["connector_name"]
    connector_module_path = app_config["connector_module_path"]
    gatekeeper_response = gatekeeper.gatekeeper_setup(connector_name, connector_module_path)
    # if gatekeeper_response.status == 1:
    #     print("something went wrong in gatekeeper setup")

    # __test_registration()
    # print(register.registered_functions)
    # # print(register.dependencies)
    # my_func = register.registered_functions[1]
    # my_func(5)
    # my_func(4)
    # my_func(1)
    # print(register.registered_functions)
    # print(register.dependencies)
    # print(get_names_registered_functions())
    # print(get_registered_dependencies())

    # return an instance of the client API?
    return client_api

def run_system(ds_config):

    # start frontend
    if ds_config["front_end"] == "fastapi":
        os.system("uvicorn fast_api:app --reload")

    return


if __name__ == "__main__":
    print("Main")
    # First parse config files and command line arguments

    # https://docs.python.org/3/library/argparse.html
    # (potentiall need to re-write some key-values from clg)
    data_station_config = parse_config(sys.argv[1])
    app_connector_config = parse_config(sys.argv[2])
    initialize_system(data_station_config, app_connector_config)

    # run_system(parse_config("data_station_config.yaml"))
