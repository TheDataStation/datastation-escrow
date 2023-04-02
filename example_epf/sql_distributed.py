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
def tpch_1():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    query = f"SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, " \
            f"sum(l_extendedprice * (1-l_discount)) as sum_disc_price, " \
            f"sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge, " \
            f"avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, " \
            f"avg(l_discount) as avg_disc, count(*) as count_order " \
            f"FROM lineitem " \
            f"WHERE l_shipdate <= date '1998-12-01' - interval '84' day " \
            f"GROUP BY l_returnflag, l_linestatus " \
            f"ORDER BY l_returnflag, l_linestatus"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_2():
    conn = duckdb.connect()
    assemble_table(conn, "part")
    assemble_table(conn, "supplier")
    assemble_table(conn, "partsupp")
    assemble_table(conn, "nation")
    assemble_table(conn, "region")
    query = f"SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment " \
            f"FROM part, supplier, partsupp, nation, region " \
            f"WHERE p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 16 " \
            f"and p_type like '%STEEL' and s_nationkey = n_nationkey and n_regionkey = r_regionkey " \
            f"and r_name = 'AFRICA' and ps_supplycost = (" \
            f"SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region " \
            f"WHERE p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey " \
            f"and n_regionkey = r_regionkey and r_name = 'AFRICA') " \
            f"order by s_acctbal desc, n_name, s_name, p_partkey limit 100"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_3():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "customer")
    assemble_table(conn, "orders")
    query = f"select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " \
            f"from customer, orders, lineitem " \
            f"where c_mktsegment = 'MACHINERY' and c_custkey = o_custkey and l_orderkey = o_orderkey " \
            f"and o_orderdate < date '1995-03-06' and l_shipdate > date '1995-03-06' " \
            f"group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_4():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "orders")
    query = f"SELECT o_orderpriority, count(*) as order_count from orders " \
            f"where o_orderdate >= date '1993-10-01' " \
            f"and o_orderdate < date '1993-10-01' + interval '3' month and exists " \
            f"(SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) " \
            f"GROUP BY o_orderpriority ORDER BY o_orderpriority"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_5():
    conn = duckdb.connect()
    assemble_table(conn, "customer")
    assemble_table(conn, "orders")
    assemble_table(conn, "lineitem")
    assemble_table(conn, "supplier")
    assemble_table(conn, "nation")
    assemble_table(conn, "region")
    query = f"SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue " \
            f"FROM customer, orders, lineitem, supplier, nation, region " \
            f"WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey " \
            f"AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey " \
            f"AND r_name = 'AMERICA' and o_orderdate >= date '1996-01-01' " \
            f"and o_orderdate < date '1996-01-01' + interval '1' year " \
            f"GROUP BY n_name ORDER BY revenue DESC"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_6():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    query = f"SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem " \
            f"where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '1' year " \
            f"and l_discount between 0.05 - 0.01 and 0.05 + 0.01 and l_quantity < 25"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_7():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "supplier")
    assemble_table(conn, "orders")
    assemble_table(conn, "customer")
    assemble_table(conn, "nation")
    query = f"SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM ( " \
            f"SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(year from l_shipdate) AS l_year, " \
            f"l_extendedprice * (1 - l_discount) AS volume " \
            f"FROM supplier, lineitem, orders, customer, nation n1, nation n2 " \
            f"WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey " \
            f"AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND (" \
            f"(n1.n_name = 'IRAN' AND n2.n_name = 'ROMANIA') OR (n1.n_name = 'ROMANIA' AND n2.n_name = 'IRAN')) " \
            f"AND l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31') AS shipping " \
            f"GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_8():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "supplier")
    assemble_table(conn, "orders")
    assemble_table(conn, "customer")
    assemble_table(conn, "nation")
    assemble_table(conn, "part")
    assemble_table(conn, "region")
    query = f"select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share " \
            f"from ( select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, " \
            f"n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region " \
            f"where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey " \
            f"and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey " \
            f"and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between date '1995-01-01' " \
            f"and date '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL' ) as all_nations " \
            f"group by o_year order by o_year"
    res = conn.execute(query).fetchall()
    return res
