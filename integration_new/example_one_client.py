import os
import shutil
import pathlib
import requests
import pickle
import time

from common import general_utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy


def call_api(username, api, exec_mode, *args, **kwargs):
    """
    Quick and dirty call_api that posts requests to the client_api
    """
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

    # if os.path.exists("data_station.db"):
    #     os.remove("data_station.db")

    # folders = ['SM_storage', 'Staging_storage']
    # for folder in folders:
    #     for filename in os.listdir(folder):
    #         file_path = os.path.join(folder, filename)
    #         if os.path.isfile(file_path) or os.path.islink(file_path):
    #             os.unlink(file_path)
    #         elif os.path.isdir(file_path):
    #             shutil.rmtree(file_path)

    # log_path = "ds_log.pkl"
    # if os.path.exists(log_path):
    #     os.remove(log_path)

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
                       'register_dataset',
                       None,
                       None,
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
    agents = [2]
    functions = ["line_count"]
    data_elements = [1, 3]
    call_api("jerry", "suggest_share", None, None, "jerry", agents, functions, data_elements)

    # Step 4: jerry acknowledges this share
    for data_id in data_elements:
        call_api("jerry", "ack_data_in_share", None, None, "jerry", data_id, 1)

    # Step 5: david calls the API line_count. He runs it in optimistic mode.
    line_count_res = call_api("david", "line_count", 1, "pessimistic")
    print("The result of line count is:", line_count_res)