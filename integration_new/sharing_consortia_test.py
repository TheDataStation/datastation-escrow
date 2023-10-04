import os
import shutil

from main import initialize_system

def cleanup():
    if os.path.exists("data_station.db"):
        os.remove("data_station.db")
    folders = ['SM_storage']
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
    ds = initialize_system(ds_config)

    # Step 1: Create new agents
    num_users = 3
    for i in range(num_users):
        ds.create_user(f"user{i}", "string", )

    # Step 2: Create and upload data elements.
    # We use the 6 csv files in integration_new/test_files/titanic_p
    for i in range(6):
        cur_f = f"integration_new/test_files/titanic_p/f{i}.csv"
        f = open(cur_f, "rb")
        plaintext_bytes = f.read()
        f.close()
        u_id = i // 2
        if i % 2 == 0:
            f_name = f"d{u_id}.csv"
        else:
            f_name = f"b{u_id}.csv"
        register_res = ds.call_api(f"user{u_id}", "register_de", f_name, "file", f_name, True, )
        ds.call_api(f"user{u_id}", "upload_de", register_res["de_id"], plaintext_bytes, )

    res = ds.call_api("user0", "list_discoverable_des")
    print(f"Result of listing discoverable data elements is {res}")

    # Step 3: Agent suggesting shares
    agents = [1]
    data_elements = [1, 2, 3, 4, 5, 6]
    s_id = ds.call_api("user0", "suggest_share", agents, data_elements, "calc_pi_and_pip")

    # Approval agent calls show_share() to see content of the share
    share_obj = ds.call_api("user1", "show_share", s_id)
    print(share_obj)

    # Step 4: Agents approving the share.
    ds.call_api(f"user0", "approve_share", s_id)
    ds.call_api(f"user1", "approve_share", s_id)
    ds.call_api(f"user2", "approve_share", s_id)

    # Step 5: user calculates pi and pip
    res_1 = ds.call_api("user0", "execute_share", s_id)
    print("Result of calculating pi and pip is:", res_1)

    # Step 6: user calls release staged to look at their pi and pip
    res = ds.call_api("user0", "release_staged")
    print(f"Releasing pi and pip for user0: {res}")

    # Step 6: users submit bids and runs the auction
    for i in range(3):
        cur_f = f"integration_new/test_files/sharing_bids/b{i+1}.txt"
        f = open(cur_f, "rb")
        plaintext_bytes = f.read()
        f.close()
        register_res = ds.call_api(f"user{i}", "register_de", f"b{i+1}.txt", "file", f"b{i+1}.txt", True, )
        ds.call_api(f"user{i}", "upload_de", register_res["de_id"], plaintext_bytes, )

    agents = [1]
    data_elements = [7, 8, 9]
    s_id = ds.call_api("user0", "suggest_share", agents, data_elements, "run_auction")

    ds.call_api(f"user0", "approve_share", s_id)
    ds.call_api(f"user1", "approve_share", s_id)
    ds.call_api(f"user2", "approve_share", s_id)

    res_2 = ds.call_api("user0", "execute_share", s_id)
    print("Result of running the auction: the winner user id is", res_2)

    # Last step: shut down the Data Station
    ds.shut_down()
