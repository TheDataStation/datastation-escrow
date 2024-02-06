from main import initialize_system
from common.general_utils import clean_test_env


if __name__ == '__main__':

    # Clean up
    clean_test_env()

    # Step 0: System initialization
    ds_config = "data_station_config.yaml"
    ds = initialize_system(ds_config)

    # Step 1: Agent creation.
    ds.create_user("bank1", "string")
    ds.create_user("bank2", "string")

    # Step 2: Register and upload DEs.
    for i in range(2):
        cur_train_de = f"integration_new/test_files/banking_p/fraud_train_{i}.csv"
        f = open(cur_train_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        register_res = ds.call_api(f"bank{i+1}", "register_de", f"fraud_train_{i}.csv", True, )
        ds.call_api(f"bank{i+1}", "upload_de", register_res["de_id"], plaintext_bytes, )
        cur_test_de = f"integration_new/test_files/banking_p/fraud_test_{i}.csv"
        f = open(cur_test_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        register_res = ds.call_api(f"bank{i + 1}", "register_de", f"fraud_test_{i}.csv", True, )
        ds.call_api(f"bank{i + 1}", "upload_de", register_res["de_id"], plaintext_bytes, )

    # Contract 1: show schema of all training data
    dest_agents = [1, 2]
    data_elements = [1, 3]
    ds.call_api("bank1", "propose_contract", dest_agents, data_elements, "show_schema")
    ds.call_api("bank1", "approve_contract", 1)
    ds.call_api("bank2", "approve_contract", 1)
    res = ds.call_api("bank1", "execute_contract", 1)
    # print(res)

    # Contract 2: an attempt to privately integrate data
    dest_agents = [1, 2]
    data_elements = [1, 3]
    ds.call_api("bank1", "propose_contract", dest_agents, data_elements, "check_column_compatibility",
                1, 3,
                ["amt", "gender", "city", "state", "city_pop_thousands", "dob"],
                ["dollar_amount", "gender", "city", "state", "city_population", "customer_date_of_birth"])
    ds.call_api("bank1", "approve_contract", 2)
    ds.call_api("bank2", "approve_contract", 2)
    res = ds.call_api("bank1", "execute_contract", 2)
    # print(res)

    # Contract 3: Bank1 thinks that he understands more about the columns, but want to see more.
    # So he requests a sample. This contract will not get approved, because the other bank does not want to show sample.
    dest_agents = [1]
    data_elements = [3]
    ds.call_api("bank1", "propose_contract", dest_agents, data_elements, "share_sample",
                3, 50)
    # ds.call_api("bank2", "approve_contract", 3)
    # res = ds.call_api("bank1", "execute_contract", 3)
    # print(res)

    # Contract 4: Bank2 instead uploads a new synthetic dataset, give it to bank1 for integration purposes.
    cur_train_de = f"integration_new/test_files/banking_p/synth_train_1.csv"
    f = open(cur_train_de, "rb")
    plaintext_bytes = f.read()
    f.close()
    register_res = ds.call_api(f"bank2", "register_de", f"synth_train_1.csv", True, )
    ds.call_api(f"bank2", "upload_de", register_res["de_id"], plaintext_bytes, )
    dest_agents = [1]
    data_elements = [5]
    ds.call_api("bank2", "propose_contract", dest_agents, data_elements, "share_de", 5)
    ds.call_api("bank2", "approve_contract", 4)
    res = ds.call_api("bank1", "execute_contract", 4)
    # print(res)

    # Bank1 looks at the synthetic dataset bank2 shared, and decide on the format of DEs both of them should use
    # for training the model.
    # The schema is: gender (1/0), lat, long, age, merch_lat, merch_long, city_pop_thousands, amt (all number)
    # Bank1 also tells bank2 to convert their data into this format and upload.
    # Let's assume they finished the transformation locally.
    for i in range(2):
        cur_train_de = f"integration_new/test_files/banking_p/clean_train_{i}.csv"
        f = open(cur_train_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        register_res = ds.call_api(f"bank{i+1}", "register_de", f"clean_train_{i}.csv", True, )
        ds.call_api(f"bank{i+1}", "upload_de", register_res["de_id"], plaintext_bytes, )
        cur_test_de = f"integration_new/test_files/banking_p/clean_test_{i}.csv"
        f = open(cur_test_de, "rb")
        plaintext_bytes = f.read()
        f.close()
        register_res = ds.call_api(f"bank{i + 1}", "register_de", f"clean_test_{i}.csv", True, )
        ds.call_api(f"bank{i + 1}", "upload_de", register_res["de_id"], plaintext_bytes, )

    # The final contract: combine the data and train the model. TODO


    # Last step: shut down the Data Station
    ds.shut_down()
