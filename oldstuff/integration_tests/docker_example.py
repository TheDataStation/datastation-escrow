import pathlib
import os
import shutil

from common import general_utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy
from ds import DataStation

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # Step 0: System initialization

    ds_config = general_utils.parse_config("data_station_config.yaml")
    app_config = general_utils.parse_config("app_connector_config.yaml")

    ds_storage_path = str(pathlib.Path(ds_config["storage_path"]).absolute())
    mount_point = str(pathlib.Path(ds_config["mount_path"]).absolute())

    ds = DataStation(ds_config, app_config)

    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    folders = ['SM_storage', 'Staging_storage']
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    # Step 1: We create two new users of the Data Station: jerry and david
    ds.create_user(User(user_name="jerry", password="string"))
    ds.create_user(User(user_name="david", password="123456"))

    # Step 2: Jerry logs in and uploads three datasets
    # He uploads DE1 and DE3 in sealed mode, and uploads DE2 in enclave mode.
    for cur_num in range(3):
        cur_file_index = (cur_num % 6) + 1
        cur_full_name = "integration_tests/test_file_full_trust/train-" + str(cur_file_index) + ".csv"
        cur_file = open(cur_full_name, "rb")
        cur_file_bytes = cur_file.read()
        cur_optimistic_flag = False
        if cur_num == 1:
            cur_optimistic_flag = True
        name_to_upload = "file-" + str(cur_num + 1)
        cur_res = ds.register_de("jerry",
                                 name_to_upload,
                                 cur_file_bytes,
                                    "file",
                                 cur_optimistic_flag,
                                 )

    # Step 3: jerry creates a policy saying that david can discover how many lines his files have
    # for DE [1, 3]. He creates another policy saying david can read the first n lines of DE [2, 3]
    policy_one = Policy(user_id=2, api="line_count", data_id=1)
    ds.upload_policy("jerry", policy_one,)
    policy_two = Policy(user_id=2, api="line_count", data_id=3)
    ds.upload_policy("jerry", policy_two,)
    policy_three = Policy(user_id=2, api="get_first_n_lines", data_id=2)
    ds.upload_policy("jerry", policy_three,)
    policy_four = Policy(user_id=2, api="get_first_n_lines", data_id=3)
    ds.upload_policy("jerry", policy_four,)

    # Step 4: david logs in and looks at the available apis
    list_of_apis = ds.get_all_apis()
    # print("All available APIs are:")
    # print(list_of_apis)

    # Step 5: david calls the API line_count. He runs it in optimistic mode.
    line_count_res = ds.call_api("david", "line_count", "optimistic")
    print("The result of line count is:", line_count_res)

    # line_count_res = client_api.call_api("line_count", cur_token, "optimistic")
    # print(line_count_res)

    # # Step 6: Jerry now adds a policy to allow david to access DE 2 for line_count
    # # Then david logs in again
    # # cur_token = client_api.login_user("jerry", "string")["access_token"]
    # policy_five = Policy(user_id=2, api="line_count", data_id=2)
    # ds.upload_policy("jerry",policy_five)
    # # cur_token = client_api.login_user("david", "123456")["access_token"]
    #
    # # Step 7: david tries to release the staged DE.
    # release_staged_res = ds.release_staged_DE("david",1)
    # print(release_staged_res)

    # # Step 8: david calls the API get_first_n_lines
    # first_line_res = client_api.call_api("get_first_n_lines", cur_token, "pessimistic", 1, DE_id=[2, 3])
    # print(first_line_res)
    #
    # # Step 9: checking the auditable log
    # client_api.read_full_log()

    # time.sleep(5)
    # Last step: shut down the Data Station
    ds.shut_down()

