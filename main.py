import os
import sys
from storagemanager.storage_manager import StorageManager
from clientapi.client_api import ClientAPI
from common.utils import parse_config

def initialize_system(ds_config):

    # In this function we set up all components that need to be initialized

    # set up an instance of the storage_manager
    storage_path = ds_config["storage_path"]
    storage_manager = StorageManager(storage_path)

    # lastly, set up an instance of the client_api
    client_api = ClientAPI(storage_manager)

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
    initialize_system(data_station_config)

    # run_system(parse_config("data_station_config.yaml"))
