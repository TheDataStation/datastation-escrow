from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI
import duckdb

@api_endpoint
def register_data(username,
                  data_name,
                  data_type,
                  access_param,
                  optimistic):
    print("This is a customized register data!")
    return EscrowAPI.register_data(username, data_name, data_type, access_param, optimistic)

@api_endpoint
def upload_data(username,
                data_id,
                data_in_bytes):
    return EscrowAPI.upload_data(username, data_id, data_in_bytes)

@api_endpoint
def suggest_share(username, agents, functions, data_elements):
    return EscrowAPI.suggest_share(username, agents, functions, data_elements)

@api_endpoint
def ack_data_in_share(username, data_id, share_id):
    return EscrowAPI.ack_data_in_share(username, data_id, share_id)

def get_data(de):
    if de.type == "file":
        return f"'{de.access_param}'"

def assemble_table(conn, table_name):
    accessible_de = EscrowAPI.get_all_accessible_des()
    first_partition_flag = True
    for de in accessible_de:
        if de.name == f"{table_name}.csv":
            table_path = get_data(de)
            if first_partition_flag:
                query = f"CREATE TABLE {table_name} AS SELECT * FROM {table_path}"
                conn.execute(query)
                first_partition_flag = False
            else:
                query = f"INSERT INTO {table_name} SELECT * FROM {table_path}"
                conn.execute(query)

@api_endpoint
@function
def select_star(table_name):
    """run select * from a table"""
    # Note: creating conn here, because we need to the same in-memory database
    conn = duckdb.connect()
    assemble_table(conn, table_name)
    query = f"SELECT * FROM {table_name}"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def join_table():
    """perform a join between two tables"""
    conn = duckdb.connect()
    assemble_table(conn, "salary")
    assemble_table(conn, "customer")
    query = "SELECT PersonID, Department, Gender " \
            "FROM salary, customer " \
            "WHERE salary.PersonID = customer.CustomerID"
    res = conn.execute(query).fetchall()
    return res
