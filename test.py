# This script loads a complete state of the data station

import main
import os
import shutil
from common import utils
from models.user import *
from models.policy import *

ds_config = utils.parse_config("data_station_config.yaml")
app_config = utils.parse_config("app_connector_config.yaml")

client_api = main.initialize_system(ds_config, app_config)

# Adding new users

client_api.create_user(User(user_name="jerry", password="string"))
client_api.create_user(User(user_name="lucy", password="123456"))
client_api.create_user(User(user_name="david", password="string"))

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

list_of_files = ["train-1.csv", "train-2.csv", "train-3.csv", "train-4.csv", "train-5.csv", "train-6.csv"]
list_of_data_ids = []

for cur_name in list_of_files:
    cur_full_name = "test_file/" + cur_name
    cur_file = open(cur_full_name, "rb")
    cur_file_bytes = cur_file.read()
    cur_res = client_api.upload_dataset(cur_name.split(".")[0], cur_file_bytes, cur_token)
    list_of_data_ids.append(cur_res.data_id)
    cur_file.close()

# Upload Policies
client_api.upload_policy(Policy(user_id=1, api="preprocess", data_id=list_of_data_ids[0]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="preprocess", data_id=list_of_data_ids[1]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="preprocess", data_id=list_of_data_ids[2]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="modeltrain", data_id=list_of_data_ids[3]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="modeltrain", data_id=list_of_data_ids[4]), cur_token)
client_api.upload_policy(Policy(user_id=1, api="predict", data_id=list_of_data_ids[5]), cur_token)

# Check accessible data
client_api.get_accessible_data(1, "preprocess")
client_api.get_accessible_data(1, "modeltrain")
client_api.get_accessible_data(1, "predict")

# call available APIs
client_api.call_api("preprocess")
client_api.call_api("modeltrain")
client_api.call_api("predict", 10, 5)
