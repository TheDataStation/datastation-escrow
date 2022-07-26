import pathlib
import main
import os
import shutil

from common import general_utils
from common.pydantic_models.user import User
from common.pydantic_models.policy import Policy

if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # Step 0: System initialization

    ds_config = general_utils.parse_config("data_station_config.yaml")
    app_config = general_utils.parse_config("app_connector_config.yaml")

    ds_storage_path = str(pathlib.Path(ds_config["storage_path"]).absolute())
    mount_point = str(pathlib.Path(ds_config["mount_path"]).absolute())

    client_api = main.initialize_system(ds_config, app_config)

    log_path = client_api.log.log_path
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
    client_api.create_user(User(user_name="jerry", password="string"))
    client_api.create_user(User(user_name="david", password="123456"))

    # Step 2: Jerry logs in and uploads three datasets
    # He uploads DE1 and DE3 in sealed mode, and uploads DE2 in enclave mode.
    cur_token = client_api.login_user("jerry", "string")["access_token"]
    for cur_num in range(3):
        cur_file_index = (cur_num % 6) + 1
        cur_full_name = "integration_tests/test_file_full_trust/train-" + str(cur_file_index) + ".csv"
        cur_file = open(cur_full_name, "rb")
        cur_file_bytes = cur_file.read()
        cur_optimistic_flag = False
        if cur_num == 1:
            cur_optimistic_flag = True
        name_to_upload = "file-" + str(cur_num + 1)
        cur_res = client_api.upload_dataset(name_to_upload,
                                            cur_file_bytes,
                                            "file",
                                            cur_optimistic_flag,
                                            cur_token, )

    # Step 3: jerry creates a policy saying that david can discover how many lines his files have
    # for DE [1, 3]. He creates another policy saying david can read the first n lines of DE [2, 3]
    policy_one = Policy(user_id=2, api="line_count", data_id=1)
    client_api.upload_policy(policy_one, cur_token)
    policy_two = Policy(user_id=2, api="line_count", data_id=3)
    client_api.upload_policy(policy_two, cur_token)
    policy_three = Policy(user_id=2, api="get_first_n_lines", data_id=2)
    client_api.upload_policy(policy_three, cur_token)
    policy_four = Policy(user_id=2, api="get_first_n_lines", data_id=3)
    client_api.upload_policy(policy_four, cur_token)

    # Step 4: david logs in and looks at the available apis
    cur_token = client_api.login_user("david", "123456")["access_token"]
    # list_of_apis = client_api.get_all_apis(cur_token)
    # print("All available APIs are:")
    # print(list_of_apis)

    # Step 5: david calls the API line_count. He runs it in optimistic mode.
    line_count_res = client_api.call_api("line_count", cur_token, "optimistic")
    print(line_count_res)

    line_count_res = client_api.call_api("line_count", cur_token, "optimistic")
    print(line_count_res)

    # # Step 6: david calls the API get_first_n_lines
    # first_line_res = client_api.call_api("get_first_n_lines", cur_token, "pessimistic", 1, DE_id=[2, 3])
    # print(first_line_res)

    # # Step 7: checking the auditable log
    # client_api.read_full_log()

    # Last step: shut down the Data Station
    client_api.shut_down(ds_config)

