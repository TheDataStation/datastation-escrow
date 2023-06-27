import os
import shutil

from main import initialize_system

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

    ds_config = "data_station_config.yaml"
    app_config = "app_connector_config.yaml"

    ds = initialize_system(ds_config, app_config)

    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    # Step 1: We create two new users of the Data Station: jerry and david
    ds.create_user("jerry", "string")
    ds.create_user("david", "123456")

    # Step 2: Jerry logs in and uploads three datasets
    # He uploads DE1 and DE3 in sealed mode, and uploads DE2 in enclave mode.
    for cur_num in range(3):
        cur_file_index = (cur_num % 6) + 1
        cur_full_name = "integration_new/test_files/titanic_p/train-" + str(cur_file_index) + ".csv"
        cur_file = open(cur_full_name, "rb")
        cur_file_bytes = cur_file.read()
        cur_optimistic_flag = False
        if cur_num == 1:
            cur_optimistic_flag = True
        name_to_upload = "file-" + str(cur_num + 1)
        register_res = ds.call_api("jerry",
                                   "register_de",
                                   None,
                                   None,
                                   "jerry",
                                   name_to_upload,
                                   "file",
                                   name_to_upload,
                                   cur_optimistic_flag, )
        upload_res = ds.call_api("jerry",
                                 "upload_de",
                                 None,
                                 None,
                                 "jerry",
                                 register_res.de_id,
                                 cur_file_bytes, )

    # Step 3: jerry suggests a share saying david can discovery how many lines his files have
    # for DE [1, 3].
    agents = [2]
    functions = ["line_count"]
    data_elements = [1, 3]
    ds.call_api("jerry", "suggest_share", None, None, "jerry", agents, functions, data_elements)

    # Step 4: jerry acknowledges this share
    for data_id in data_elements:
        ds.call_api("jerry", "approve_share", None, None, "jerry", data_id, 1)

    # Step 5: david calls the API line_count. He runs it in optimistic mode.
    line_count_res = ds.call_api("david", "line_count", 1, "pessimistic")
    print("The result of line count is:", line_count_res)

    # Last step: shut down the Data Station
    ds.shut_down()
