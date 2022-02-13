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
from crypto import cryptoutils as cu

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

    # Save data station's public key
    ds_public_key = client_api.key_manager.ds_public_key

    # print("Data Station's public key is:")
    # print(ds_public_key)

    # cur_time = time.time()
    # print("System initialization done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # Adding new users

    list_of_users = ["jerry", "lucy", "david"]

    # In no trust mode, when a user registers, he needs to upload his symmetric key and public key as well

    for cur_user in list_of_users:
        # Create a new symmetric key
        sym_key = cu.generate_symmetric_key()
        # print("Symmetric in the plain is:")
        # print(sym_key)

        # Create a new private/public key pair
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        # print("User's public key is:")
        # print(cur_public_key)

        # Now we encrypt the symmetric key with data station's public key
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        # print("Cipher symmetric key is:")
        # print(cipher_sym_key)

        # Quickly check to see if everything works correctly
        # dec_sym_key = cu.decrypt_data_with_private_key(cipher_sym_key, client_api.key_manager.ds_private_key)
        # print("Decrypted symmetric key is:")
        # print(dec_sym_key)

        # TODO: Now uploading both the user's public key, and the encrypted symmetric key to the DS
        client_api.create_user(User(user_name=cur_user, password="string"), cipher_sym_key, cur_public_key)

    # Taking a look at the keys that are stored
    # print(client_api.key_manager.agents_symmetric_key)
    # print(client_api.key_manager.agents_public_key)

    # cur_time = time.time()
    # print("User addition done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # Log in a user to get a token

    cur_token = client_api.login_user("jerry", "string")["access_token"]

    # cur_time = time.time()
    # print("Log in done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # Look at all available APIs and APIDependencies

    list_of_apis = client_api.get_all_apis(cur_token)
    list_of_api_dependencies = client_api.get_all_api_dependencies(cur_token)
    print(list_of_apis)
    print(list_of_api_dependencies)

    # cur_time = time.time()
    # print("Looking at dependency graph done")
    # print("--- %s seconds ---" % (cur_time - prev_time))
    # prev_time = cur_time

    # Upload datasets

    # TODO: before we test uploading datasets,
    #       we first have to create the encrypted files using the newly generated symmetric keys
