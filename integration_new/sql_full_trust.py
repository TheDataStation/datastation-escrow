import os
import shutil
import csv
import math

from main import initialize_system

def data_gen(num_partitions):
    """
    This function generates tpch data for testing different silos.
    Output goes to integration_new/test_files/sql_plain.
    """
    # Define input/output directories
    input_dir = "tpch_data/1MB/"
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
        else:
            cur_partitions = num_partitions
        with open(f"{input_dir}{cur_table}.csv", 'r') as f:
            reader = csv.reader(f)
            total_rows = sum(1 for row in reader)
            rows_per_file = int(math.ceil(total_rows / cur_partitions))
            f.seek(0)
            for i in range(cur_partitions):
                output_file = f"{output_dir}{cur_table}{i}.csv"
                with open(output_file, 'w', newline='') as f_out:
                    writer = csv.writer(f_out)
                    cur_schema = tpch_schemas[cur_table]
                    writer.writerow(cur_schema)
                    for j in range(rows_per_file):
                        try:
                            row = next(reader)
                            writer.writerow(row)
                        except StopIteration:
                            break

def cleanup():
    folders = ['SM_storage',
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

    # Step 0: System initialization

    ds_config = "data_station_config.yaml"
    app_config = "app_connector_config.yaml"

    ds = initialize_system(ds_config, app_config)

    # Step 1: We create two new users of the Data Station
    num_users = 3
    for i in range(num_users):
        ds.create_user(f"user{i}", "string", )

    # Step 2: Each user uploads their share of the dataset.
    data_gen(num_users)

    # Create the TPCH files, and upload them.

    partitioned_tables = ["customer", "lineitem", "orders", "part", "partsupp", "supplier"]
    small_tables = ["nation", "region"]
    for tbl in small_tables:
        filename = f"integration_new/test_files/sql_plain/{tbl}0.csv"
        f = open(filename, "rb")
        plaintext_bytes = f.read()
        f.close()
        register_res = ds.call_api(f"user0", "register_de", None, None, f"user0",
                                   f"{tbl}.csv", "file", f"{tbl}.csv", True, )
        ds.call_api(f"user0", "upload_de", None, None, f"user0",
                    register_res.de_id, plaintext_bytes, )
    for i in range(num_users):
        for tbl in partitioned_tables:
            filename = f"integration_new/test_files/sql_plain/{tbl}{i}.csv"
            f = open(filename, "rb")
            plaintext_bytes = f.read()
            f.close()
            register_res = ds.call_api(f"user{i}", "register_de", None, None, f"user{i}",
                                       f"{tbl}.csv", "file", f"{tbl}.csv", False, )
            ds.call_api(f"user{i}", "upload_de", None, None, f"user{i}",
                        register_res.de_id, plaintext_bytes, )

    res = ds.call_api("user0", "list_discoverable_des", None, None, "user0")
    print(f"Result of listing discoverable data elements is {res}")

    # Step 3: user0 suggests a share saying he can run all functions in share
    agents = [1]
    total_des = num_users * len(partitioned_tables) + len(small_tables)
    data_elements = list(range(1, total_des + 1))
    ds.call_api("user0", "suggest_share", None, None, "user0", agents, data_elements, "select_star",
                "nation", message="hello select star")
    ds.call_api("user0", "suggest_share", None, None, "user0", agents, data_elements, "tpch_1")
    ds.call_api("user0", "suggest_share", None, None, "user0", agents, data_elements, "tpch_2")

    # User1 calls show_share() to see what's the content of shares
    for i in range(1, 4):
        share_obj = ds.call_api("user1", "show_share", None, None, "user0", i)
        print(share_obj)

    # Step 4: all users acknowledge the share
    for i in range(num_users):
        for j in range(1, 4):
            ds.call_api(f"user{i}", "approve_share", None, None, f"user{i}", j)

    # Step 5: user calls execute share
    select_star_res = ds.call_api("user0", "execute_share", None, None, "user0", 1)
    print("Result of select star from nation is:", select_star_res)

    tpch_1_res = ds.call_api("user0", "execute_share", None, None, "user0", 2)
    print("Result of TPCH 1 is", tpch_1_res)

    tpch_2_res = ds.call_api("user0", "execute_share", None, None, "user0", 3)
    print("Result of TPCH 2 is", tpch_2_res)

    # Last step: shut down the Data Station
    ds.shut_down()
