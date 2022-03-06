import os
import pathlib
import main

from models.user import *
from models.policy import *
from common import utils

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

    # We upload 8 users, one holds each partition of the data (X and y)
    num_users = 1

    for cur_num in range(num_users):
        cur_uname = "user" + str(cur_num)
        client_api.create_user(User(user_name=cur_uname, password="string"))

    # Use token for user0
    cur_token = client_api.login_user("user0", "string")["access_token"]
    # client_api.call_api("ddprofiler", cur_token, "pessimistic")
    # client_api.call_api("nbc", cur_token, "pessimistic")
    client_api.call_api("keywords_search", cur_token, "pessimistic")

    client_api.shut_down(ds_config)
