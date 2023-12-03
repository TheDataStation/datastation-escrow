from main import initialize_system
from common.general_utils import clean_test_env


if __name__ == '__main__':

    # Clean up
    clean_test_env()

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
        print(register_res)
        ds.call_api(f"user{u_id}", "upload_de", register_res["de_id"], plaintext_bytes, )

    res = ds.call_api("user0", "list_discoverable_des_with_src")
    print(f"Listing discoverable data elements with their source agents: {res}")

    exit()

    # Step 3: Agent suggesting shares
    agents = [1]
    data_elements = [4]
    res = ds.call_api("user0", "propose_contract", agents, data_elements, "print_first_row", 4)
    print(res)

    # Approval agent calls show_share() to see content of the share
    share_obj = ds.call_api("user1", "show_contract", 1)
    print(share_obj)

    # Step 4: Agents approving the share.
    res = ds.call_api(f"user1", "approve_contract", 1)
    print(res)

    # Step 5: user calls execute contract
    print_first_row_res = ds.call_api("user0", "execute_contract", 1)
    print("Result of printing first row is:", print_first_row_res)

    # Last step: shut down the Data Station
    ds.shut_down()
