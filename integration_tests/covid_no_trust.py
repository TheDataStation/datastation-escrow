import pathlib
import glob
import main
import os
import shutil
import PIL.Image as Image
import numpy as np
import pickle

from common import utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy
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

    # Adding new users

    # In the proto version of the code, we upload 4 users, each holds on image
    num_users = 4

    # We generate keys outside the loop because we don't want to count its time

    cipher_sym_key_list = []
    public_key_list = []

    for cur_num in range(num_users):
        sym_key = cu.generate_symmetric_key()
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)
        cur_uname = "user" + str(cur_num)
        client_api.create_user(User(user_name=cur_uname, password="string"),
                               cipher_sym_key_list[cur_num],
                               public_key_list[cur_num], )

    # print(client_api.key_manager.agents_symmetric_key)

    # First clear ml_file_no_trust/training

    no_trust_folder = 'integration_tests/ml_file_no_trust/training_covid'
    for filename in os.listdir(no_trust_folder):
        file_path = os.path.join(no_trust_folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Clear the storage place

    folder = 'SM_storage'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    # Now we create the encrypted files. We keep track of which users upload which files using
    # a user counter.

    cur_username = "user0"
    cur_user_index = 1
    cur_user_file_ctr = 0
    origin_image_list = glob.glob("integration_tests/ml_file_full_trust/training_covid/*")
    num_files = len(origin_image_list)
    # Keeping track of how many files we want each user to upload
    user_file_limit = int(num_files/num_users)+1

    # in the beginning, we log in the first user
    cur_token = client_api.login_user(cur_username, "string")["access_token"]

    for cur_image_path in origin_image_list:

        # Check if currently logged-in user has reached his file upload limit.
        # If yes, we log in a new user.
        if cur_user_file_ctr >= user_file_limit:
            cur_username = "user" + str(cur_user_index)
            cur_token = client_api.login_user(cur_username, "string")["access_token"]
            cur_user_index += 1
            cur_user_file_ctr = 0

        # Creating the encrypted image and save its np array to pkl.
        cur_image_name = cur_image_path.split("/")[-1]
        cur_user_sym_key = client_api.key_manager.agents_symmetric_key[cur_user_index]
        # Convert image to RGB, and resize it
        cur_image = Image.open(cur_image_path).convert('RGB').resize((200, 200))
        # Store image as np array
        img_data = np.array(cur_image)
        # np object to pkl bytes
        cur_pkl_obj = pickle.dumps(img_data)
        # pkl bytes encrypted
        ciphertext_bytes = cu.encrypt_data_with_symmetric_key(cur_pkl_obj, cur_user_sym_key)
        # write encrypted bytes to file
        cur_cipher_name = "integration_tests/ml_file_no_trust/training_covid/" \
                          + cur_image_name + ".pkl"
        cur_cipher_file = open(cur_cipher_name, "wb")
        cur_cipher_file.write(ciphertext_bytes)
        cur_cipher_file.close()

        # Increment cur_user_file_ctr
        cur_user_file_ctr += 1

        # Upload the current encrypted file to the DS
        cur_upload_res = client_api.upload_dataset(cur_image_name,
                                                   ciphertext_bytes,
                                                   "file",
                                                   False,
                                                   cur_token, )

        # Add a policy saying user with id==1 can call train_covid_model on the uploaded dataset
        client_api.upload_policy(Policy(user_id=1, api="train_covid_model", data_id=cur_upload_res.data_id), cur_token)

    # We proceed to actually calling the model training API

    # Use token for user0
    cur_token = client_api.login_user("user0", "string")["access_token"]

    client_api.call_api("train_covid_model", cur_token, "optimistic")

    # Shutting down

    client_api.shut_down(ds_config)

