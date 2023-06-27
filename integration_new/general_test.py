import os
import shutil
import csv
import math

from main import initialize_system

def cleanup():
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


if __name__ == '__main__':

    # Clean up
    cleanup()

    # Step 0: System initialization
    ds_config = "data_station_config.yaml"
    app_config = "app_connector_config.yaml"
    ds = initialize_system(ds_config, app_config)

    # Step 1: Create new users
    num_users = 2
    for i in range(num_users):
        ds.create_user(f"user{i}", "string", )

    # Step 2: Create and upload data elements.
    # We use the 6 csv files in integration_new/test_files/titanic_p
    for i in range(6):
        cur_f = f"integration_new/test_files/titanic_p/f{i}.csv"
        f = open(cur_f, "rb")
        plaintext_bytes = f.read()
        f.close()
        u_id = i // 3
        register_res = ds.call_api(f"user{u_id}", "register_de", None, None,
                                   f"user{u_id}", f"f{i}.csv", "file", f"f{i}.csv", True, )
        ds.call_api(f"user{u_id}", "upload_de", None, None,
                    f"user{u_id}", register_res.de_id, plaintext_bytes, )

    res = ds.call_api("user0", "list_discoverable_des", None, None, "user0")
    print(f"Result of listing discoverable data elements is {res}")

    # # Step 3: user0 suggests a share saying he can run all functions in share
    # agents = [1]
    # total_des = num_users * len(partitioned_tables) + len(small_tables)
    # data_elements = list(range(1, total_des + 1))
    # ds.call_api("user0", "suggest_share", None, None, "user0", agents, data_elements, "select_star",
    #             "nation", message="hello select star")
    # ds.call_api("user0", "suggest_share", None, None, "user0", agents, data_elements, "tpch_1")
    # ds.call_api("user0", "suggest_share", None, None, "user0", agents, data_elements, "tpch_2")
    #
    # # User1 calls show_share() to see what's the content of shares
    # for i in range(1, 4):
    #     share_obj = ds.call_api("user1", "show_share", None, None, "user0", i)
    #     print(share_obj)
    #
    # # Step 4: all users acknowledge the share
    # for i in range(num_users):
    #     for j in range(1, 4):
    #         ds.call_api(f"user{i}", "approve_share", None, None, f"user{i}", j)
    #
    # # Step 5: user calls execute share
    # select_star_res = ds.call_api("user0", "execute_share", None, None, "user0", 1)
    # print("Result of select star from nation is:", select_star_res)
    #
    # tpch_1_res = ds.call_api("user0", "execute_share", None, None, "user0", 2)
    # print("Result of TPCH 1 is", tpch_1_res)
    #
    # tpch_2_res = ds.call_api("user0", "execute_share", None, None, "user0", 3)
    # print("Result of TPCH 2 is", tpch_2_res)

    # Last step: shut down the Data Station
    ds.shut_down()
