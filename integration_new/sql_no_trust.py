import os
import sys
import shutil
import csv
import math
import time

from main import initialize_system
from crypto import cryptoutils as cu


def data_gen(num_partitions, data_dir):
    """
    This function generates tpch data for testing different silos.
    Output goes to integration_new/test_files/sql_plain.
    """
    # Define input/output directories
    input_dir = f"tpch_data/{data_dir}/"
    output_dir = "integration_new/test_files/sql_plain/"
    # Create a dictionary to store the schemas
    tpch_schemas = {"customer": ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal",
                                 "c_mktsegment", "c_comment"],
                    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
                                 "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
                                 "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct",
                                 "l_shipmode", "l_comment"],
                    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
                    "orders": ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
                               "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"],
                    "part": ["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size",
                             "p_container", "p_retailprice", "p_comment"],
                    "partsupp": ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"],
                    "region": ["r_regionkey", "r_name", "r_comment"],
                    "supplier": ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal",
                                 "s_comment"]}
    table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    small_tables = ["nation", "region"]
    for cur_table in table_names:
        if cur_table in small_tables:
            cur_partitions = 1
            num_writes = 1
        else:
            cur_partitions = num_partitions
            num_writes = 2
        with open(f"{input_dir}{cur_table}.csv", 'r') as f:
            reader = csv.reader(f)
            total_rows = sum(1 for row in reader)
            print(f"{cur_table}")
            print(total_rows)
            rows_per_file_per_write = int(math.floor(total_rows / cur_partitions / num_writes))
            print(rows_per_file_per_write)
            f.seek(0)
            for i in range(cur_partitions):
                output_file = f"{output_dir}{cur_table}{i}.csv"
                for j in range(num_writes):
                    with open(output_file, 'a+', newline='') as f_out:
                        writer = csv.writer(f_out)
                        if j == 0:
                            cur_schema = tpch_schemas[cur_table]
                            writer.writerow(cur_schema)
                        for k in range(rows_per_file_per_write):
                            try:
                                row = next(reader)
                                writer.writerow(row)
                            except StopIteration:
                                break


def cleanup():
    folders = [
        'SM_storage',
        'Staging_storage',
        "integration_new/test_files/sql_plain",
        "integration_new/test_files/sql_cipher",
    ]
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)


if __name__ == '__main__':

    if os.path.exists("data_station.db"):
        os.remove("data_station.db")

    # Clean up
    cleanup()

    # Step 0: Set some configs, and generate data
    num_users = int(sys.argv[1])
    data_dir = sys.argv[2]
    workload = sys.argv[3]
    iterations = int(sys.argv[4])
    data_gen(num_users, data_dir)

    # Step 0: System initialization

    ds_config = "data_station_config.yaml"
    app_config = "app_connector_config.yaml"

    ds = initialize_system(ds_config, app_config)

    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    wal_path = ds.write_ahead_log.wal_path
    if os.path.exists(log_path):
        os.remove(log_path)

    ds_public_key = ds.key_manager.ds_public_key
    # print(ds_public_key)

    # Step 1: We create two new users of the Data Station

    # Generate keys for the users
    cipher_sym_key_list = []
    public_key_list = []

    for i in range(num_users):
        sym_key = b'oHRZBUvF_jn4N3GdUpnI6mW8mts-EB7atAUjhVNMI58='
        cipher_sym_key = cu.encrypt_data_with_public_key(sym_key, ds_public_key)
        cipher_sym_key_list.append(cipher_sym_key)
        cur_private_key, cur_public_key = cu.generate_private_public_key_pair()
        public_key_list.append(cur_public_key)
        ds.create_user(f"user{i}", "string", cipher_sym_key_list[i], public_key_list[i], )

    # print(ds.key_manager.agents_symmetric_key)

    # Step 2: Each user uploads their share of the dataset.

    # Create the encrypted TPCH files, and upload them.

    partitioned_tables = ["customer", "lineitem", "orders", "part", "partsupp", "supplier"]
    small_tables = ["nation", "region"]
    for tbl in small_tables:
        filename = f"integration_new/test_files/sql_plain/{tbl}0.csv"
        f = open(filename, "rb")
        plaintext_bytes = f.read()
        f.close()
        cur_user_sym_key = ds.key_manager.agents_symmetric_key[1]
        ciphertext_bytes = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(plaintext_bytes)
        register_res = ds.call_api(f"user0", "register_de", None, None, f"user0",
                                   f"{tbl}.csv", "file", f"{tbl}.csv", False, )
        ds.call_api(f"user0", "upload_de", None, None, f"user0",
                    register_res.de_id, ciphertext_bytes, )
    for i in range(num_users):
        for tbl in partitioned_tables:
            filename = f"integration_new/test_files/sql_plain/{tbl}{i}.csv"
            f = open(filename, "rb")
            plaintext_bytes = f.read()
            f.close()
            # print(f"{tbl}")
            # print(f"Plaintext size is {len(plaintext_bytes)}")
            cur_user_sym_key = ds.key_manager.agents_symmetric_key[i + 1]
            ciphertext_bytes = cu.get_symmetric_key_from_bytes(cur_user_sym_key).encrypt(plaintext_bytes)
            # print(f"Ciphertext size is {len(ciphertext_bytes)}")
            register_res = ds.call_api(f"user{i}", "register_de", None, None, f"user{i}",
                                       f"{tbl}.csv", "file", f"{tbl}.csv", False, )
            ds.call_api(f"user{i}", "upload_de", None, None, f"user{i}",
                        register_res.de_id, ciphertext_bytes, )

    # Step 3: user0 suggests a share saying he can run all functions in share
    agents = [1]
    if workload == "tpch":
        functions = ["tpch_1", "tpch_2", "tpch_3", "tpch_4", "tpch_5", "tpch_6",
                     "tpch_7", "tpch_8", "tpch_9", "tpch_10", "tpch_11", "tpch_12", "tpch_13", "tpch_14", "tpch_15",
                     "tpch_16", "tpch_17", "tpch_18", "tpch_19", "tpch_20", "tpch_21", "tpch_22"]
    elif workload == "conclave":
        functions = ["conclave_1", "conclave_2", "conclave_3"]
    else:
        functions = ["secyan_1", "secyan_2", "secyan_3", "secyan_4", "secyan_5"]
    total_des = num_users * len(partitioned_tables) + len(small_tables)
    data_elements = list(range(1, total_des + 1))
    ds.call_api("user0", "suggest_share", None, None, "user0", agents, functions, data_elements)

    # Step 4: all users acknowledge the share
    cur_de_id = 1
    for i in range(num_users):
        if i == 0:
            cur_user_de_count = len(partitioned_tables) + len(small_tables)
        else:
            cur_user_de_count = len(partitioned_tables)
        for j in range(cur_user_de_count):
            ds.call_api(f"user{i}", "approve_share", None, None, f"user{i}", cur_de_id, 1)
            cur_de_id += 1

    # Step 5: user0 calls functions

    # select_star_res = ds.call_api("user0", "select_star", 1, "pessimistic", "nation")
    # select_star_res = cu.from_bytes(cu.decrypt_data_with_symmetric_key(select_star_res,
    #                                                                    ds.key_manager.get_agent_symmetric_key(1)))
    # print("Result of select star from nation is:", select_star_res)

    for i in range(1, len(functions)+1):
        for j in range(iterations):
            start_time = time.time()
            query_res, dec_time = ds.call_api("user0", f"{workload}_{i}", 1, "pessimistic")
            query_res = cu.from_bytes(cu.decrypt_data_with_symmetric_key(query_res,
                                                                         ds.key_manager.get_agent_symmetric_key(1)))
            query_time = time.time() - start_time
            print(f"Total query time is {query_time}")
            print(f"Time spent for decryption is {dec_time}")
            with open(f"numbers/{workload}/{data_dir}_{num_users}.csv", "a") as file:
                writer = csv.writer(file)
                writer.writerow([i, query_time, dec_time])
            print(f"{workload}{i} done.")
            if j == 0:
                print(f"Result of {workload }{i} is:", query_res[:10])

    # Last step: shut down the Data Station
    ds.shut_down()
