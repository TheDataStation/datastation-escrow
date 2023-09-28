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
    num_users = 2
    for i in range(num_users):
        res = ds.create_user(f"user{i}", "string", )
        print(res)

    # Step 2: Create and upload data elements.
    # We use the 6 csv files in integration_new/test_files/titanic_p
    for i in range(6):
        cur_f = f"integration_new/test_files/titanic_p/f{i}.csv"
        f = open(cur_f, "rb")
        plaintext_bytes = f.read()
        f.close()
        u_id = i // 3
        register_res = ds.call_api(f"user{u_id}", "register_de", f"f{i}.csv", "file", f"f{i}.csv", True, )
        ds.call_api(f"user{u_id}", "upload_de", register_res["de_id"], plaintext_bytes, )

    res = ds.call_api("user0", "list_discoverable_des")
    print(f"Result of listing discoverable data elements is {res}")

    # Step 3: Agent suggesting shares
    agents = [1]
    data_elements = [4]
    ds.call_api("user0", "suggest_share", agents, data_elements, "print_first_row", 4)

    # Approval agent calls show_share() to see content of the share
    share_obj = ds.call_api("user1", "show_share", 1)
    print(share_obj)

    # Step 4: Agents approving the share.
    ds.call_api(f"user1", "approve_share", 1)

    # Step 5: user calls execute share
    print_first_row_res = ds.call_api("user0", "execute_share", 1)
    print("Result of printing first row is:", print_first_row_res)

    # Last step: shut down the Data Station
    ds.shut_down()
