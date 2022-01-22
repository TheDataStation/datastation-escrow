import os
import sys
import yaml

def parse_config(path_to_config):
    with open(path_to_config) as config_file:
        ds_config = yaml.load(config_file, Loader=yaml.FullLoader)
    return ds_config

def initialize_system(ds_config):

    # setup dbservice (if needed)

    # setup gatekeeper (if needed)

    # setup client_api (if needed)

    # setup storage_manager

    return

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
    run_system(data_station_config)
