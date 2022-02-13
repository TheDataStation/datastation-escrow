# This script loads a complete state of the data station
import sys
import main
import os
import shutil
import time
import math
import random

from common import utils
from models.user import *
from models.policy import *
from common.utils import parse_config
from crypto import cryptoutils

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # Get start time of the script
    prev_time = time.time()

    # Read in the configuration file
    test_config = parse_config(sys.argv[1])

    # System initialization

    ds_config = utils.parse_config("data_station_config.yaml")
    app_config = utils.parse_config("app_connector_config.yaml")

    client_api = main.initialize_system(ds_config, app_config)

    # cur_time = time.time()
    # print("System initialization done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # Adding new users

    # In no trust mode, when a user registers, he needs to upload his symmetric key and public key as well

    for _ in range(3):
        # Create a new symmetric key
        sym_key = cryptoutils.generate_symmetric_key()
        print(sym_key)

    # client_api.create_user(User(user_name="jerry", password="string"))
    # client_api.create_user(User(user_name="lucy", password="123456"))
    # client_api.create_user(User(user_name="david", password="string"))

    cur_time = time.time()
    print("User addition done")
    print("--- %s seconds ---" % (cur_time - prev_time))
    prev_time = cur_time


