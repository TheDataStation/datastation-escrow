import pathlib
import sys
import main
import os
import shutil
import time
import math
import random
from csv import writer
import numpy as np

from Interceptor import interceptor
from common import utils
from models.user import *
from models.policy import *
from common.utils import parse_config
from crypto import cryptoutils as cu

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # System initialization

    ds_config = utils.parse_config("data_station_config.yaml")
    app_config = utils.parse_config("app_connector_config.yaml")

    ds_storage_path = str(pathlib.Path(ds_config["storage_path"]).absolute())
    mount_point = str(pathlib.Path(ds_config["mount_path"]).absolute())

    client_api = main.initialize_system(ds_config, app_config)

    # Remove the code block below if testing out durability of log
    log_path = client_api.log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    # Remove the code block below if testing out durability of wal
    wal_path = client_api.write_ahead_log.wal_path
    if os.path.exists(wal_path):
        os.remove(wal_path)

    # Save data station's public key
    ds_public_key = client_api.key_manager.ds_public_key
    # print(ds_public_key)

    # Adding new users

    # We upload 8 users, one holds each partition of the data (X and y)
    num_users = 8

    # We generate keys outside the loop because we don't want to count its time

    cipher_sym_key_list = []
    public_key_list = []

    for cur_num in range(num_users):
        sym_key = cu.generate_symmetric_key()
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)

    for cur_num in range(num_users):
        cur_uname = "user" + str(cur_num+1)
        client_api.create_user(User(user_name=cur_uname, password="string"),
                               cipher_sym_key_list[cur_num],
                               public_key_list[cur_num], )

    # Adding datasets and policies

    # First clear test_file_no_trust

    no_trust_folder = 'test/ml_file_no_trust/testing_income'
    for filename in os.listdir(no_trust_folder):
        file_path = os.path.join(no_trust_folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Now we create the encrypted files

    for cur_num in range(num_users):
        cur_train_X = "test/ml_file_full_trust/training_income/train" + str(cur_num) + "_X.npy"
        cur_user_sym_key = client_api.key_manager.agents_symmetric_key[cur_num+1]
        cur_plain_file = open(cur_train_X, 'rb').read()
        ciphertext_bytes = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(cur_plain_file)
        cur_cipher_name = "test/ml_file_no_trust/testing_income/train" + str(cur_num) + "_X.npy"
        cur_cipher_file = open(cur_cipher_name, "wb")
        cur_cipher_file.write(ciphertext_bytes)
        cur_cipher_file.close()

        cur_train_y = "test/ml_file_full_trust/training_income/train" + str(cur_num) + "_y.npy"
        cur_user_sym_key = client_api.key_manager.agents_symmetric_key[cur_num + 1]
        cur_plain_file = open(cur_train_y, 'rb').read()
        ciphertext_bytes = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(cur_plain_file)
        cur_cipher_name = "test/ml_file_no_trust/testing_income/train" + str(cur_num) + "_y.npy"
        cur_cipher_file = open(cur_cipher_name, "wb")
        cur_cipher_file.write(ciphertext_bytes)
        cur_cipher_file.close()

    client_api.shut_down(ds_config)
