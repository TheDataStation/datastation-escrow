# This script loads a complete state of the data station
import sys
import main
import os
import shutil
import time

from common import utils
from models.user import *
from models.policy import *
from common.utils import parse_config

# Get start time of the script
prev_time = time.time()

# Read in the configuration file
test_config = parse_config(sys.argv[1])

# System initialization

ds_config = utils.parse_config("data_station_config.yaml")
app_config = utils.parse_config("app_connector_config.yaml")

client_api = main.initialize_system(ds_config, app_config)

cur_time = time.time()
print("System initialization done")
print("--- %s seconds ---" % (cur_time - prev_time))
prev_time = cur_time

# Adding new users

client_api.create_user(User(user_name="jerry", password="string"))
client_api.create_user(User(user_name="lucy", password="123456"))
client_api.create_user(User(user_name="david", password="string"))

cur_time = time.time()
print("User addition done")
print("--- %s seconds ---" % (cur_time - prev_time))
prev_time = cur_time

# Log in a user to get a token

cur_token = client_api.login_user("jerry", "string")["access_token"]

# Look at all available APIs and APIDependencies

apis = client_api.get_all_apis(cur_token)
api_dependencies = client_api.get_all_api_dependencies(cur_token)
print(apis)
print(api_dependencies)

# Upload datasets

# First clear the storage place

folder = 'SM_storage'
for filename in os.listdir(folder):
    file_path = os.path.join(folder, filename)
    if os.path.isfile(file_path) or os.path.islink(file_path):
        os.unlink(file_path)
    elif os.path.isdir(file_path):
        shutil.rmtree(file_path)

num_files = test_config["num_files"]
list_of_data_ids = []

for cur_num in range(num_files):
    cur_file_index = (cur_num % 6) + 1
    cur_full_name = "test/test_file/train-" + str(cur_file_index) + ".csv"
    cur_file = open(cur_full_name, "rb")
    cur_file_bytes = cur_file.read()
    cur_optimistic = False
    name_to_upload = "file-" + str(cur_num)
    cur_res = client_api.upload_dataset(name_to_upload,
                                        cur_file_bytes,
                                        "file",
                                        cur_optimistic,
                                        cur_token,)
    if cur_res.status == 0:
        list_of_data_ids.append(cur_res.data_id)
    cur_file.close()

# Upload Policies
client_api.upload_policy(Policy(user_id=1, api="preprocess", data_id=list_of_data_ids[0]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="preprocess", data_id=list_of_data_ids[1]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="preprocess", data_id=list_of_data_ids[2]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="modeltrain", data_id=list_of_data_ids[3]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="modeltrain", data_id=list_of_data_ids[4]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="predict", data_id=list_of_data_ids[5]), cur_token)

# call available APIs
client_api.call_api("preprocess", cur_token)
client_api.call_api("modeltrain", cur_token)
client_api.call_api("predict", cur_token, 10, 5)
