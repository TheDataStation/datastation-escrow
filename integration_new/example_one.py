import os
import shutil
import pathlib
import requests
import pickle
import time

from common import general_utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy
from clientapi.client_api import ClientAPI


def call_api(username, api, exec_mode, *args, **kwargs):
    api_dict = {'username': username,
                'api': api,
                'exec_mode': exec_mode,
                'args': args,
                'kwargs': kwargs}

    api_response = requests.post("http://localhost:8080/call_api",
                                 data=pickle.dumps(api_dict),
                                 # headers={'Content-Type': 'application/octet-stream'},
                                 )

    return pickle.loads(api_response.content)


if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    folders = ['SM_storage', 'Staging_storage']
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    # Step 0: System initialization

    ds_config = general_utils.parse_config("data_station_config.yaml")
    app_config = general_utils.parse_config("app_connector_config.yaml")

    ds_storage_path = str(pathlib.Path(ds_config["storage_path"]).absolute())
    mount_point = str(pathlib.Path(ds_config["mount_path"]).absolute())

    ds = ClientAPI(ds_config, app_config)

    # log_path = ds.data_station_log.log_path
    # if os.path.exists(log_path):
    #     os.remove(log_path)

    ds.start_server()

    # just to wait for server to start
    time.sleep(3)

    # START REMOTE USER LOGIN AND ACTIONS

    # Step 1: We create two new users of the Data Station: jerry and david
    u1 = {'user': User(user_name="jerry", password="string")}
    create_user_response = requests.post("http://localhost:8080/create_user",
                                         data=pickle.dumps(u1),
                                         # headers={'Content-Type': 'application/octet-stream'},
                                         )
    print(pickle.loads(create_user_response.content))
    u2 = {'user': User(user_name="david", password="123456")}
    create_user_response = requests.post("http://localhost:8080/create_user",
                                         data=pickle.dumps(u2),
                                         # headers={'Content-Type': 'application/octet-stream'},
                                         )
    print(pickle.loads(create_user_response.content))

    # Step 2: Jerry logs in and uploads three datasets
    # He uploads DE1 and DE3 in sealed mode, and uploads DE2 in enclave mode.
    for cur_num in range(3):
        cur_file_index = (cur_num % 6) + 1
        cur_full_name = "integration_tests/test_file_full_trust/train-" + \
            str(cur_file_index) + ".csv"
        cur_file = open(cur_full_name, "rb")
        cur_file_bytes = cur_file.read()
        cur_optimistic_flag = False
        if cur_num == 1:
            cur_optimistic_flag = True
        name_to_upload = "file-" + str(cur_num + 1)
        ret = call_api('jerry',
                       'upload_dataset',
                       'pessimistic',
                       "jerry",
                       name_to_upload,
                       cur_file_bytes,
                       "file",
                       cur_optimistic_flag)

        # cur_res = ds.call_api("jerry",
        #                       "upload_dataset",
        #                       "pessimistic",
        #                       ds,
        #                       "jerry",
        #                       name_to_upload,
        #                       cur_file_bytes,
        #                       "file",
        #                       cur_optimistic_flag,)

    # Step 3: jerry creates a policy saying that david can discover how many lines his files have
    # for DE [1, 3].
    policy_one = Policy(user_id=2, api="line_count", data_id=1)
    api_dict = {'username': 'jerry',
                'api': 'upload_policy',
                'exec_mode': 'pessimistic',
                'args': ["jerry", policy_one],
                'kwargs': {}}
    api_response = requests.post("http://localhost:8080/call_api",
                                 data=pickle.dumps(api_dict),
                                 # headers={'Content-Type': 'application/octet-stream'},
                                 )

    policy_two = Policy(user_id=2, api="line_count", data_id=3)
    api_dict = {'username': 'jerry',
                'api': 'upload_policy',
                'exec_mode': 'pessimistic',
                'args': ["jerry", policy_two],
                'kwargs': {}}
    api_response = requests.post("http://localhost:8080/call_api",
                                 data=pickle.dumps(api_dict),
                                 # headers={'Content-Type': 'application/octet-stream'},
                                 )

    # Step 4: david calls the API line_count. He runs it in optimistic mode.
    line_count_res = ds.call_api("david", "line_count", "optimistic")
    print("The result of line count is:", line_count_res)

    # Last step: shut down the Data Station
    ds.shut_down()
