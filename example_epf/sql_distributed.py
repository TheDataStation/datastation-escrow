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
                query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv({table_path}, " \
                        f"ignore_errors=1, auto_detect=1)"
                conn.execute(query)
                first_partition_flag = False
            else:
                query = f"INSERT INTO {table_name} SELECT * FROM read_csv({table_path}, " \
                        f"ignore_errors=1, auto_detect=1)"
                conn.execute(query)

def assemble_orders(conn):
    accessible_de = EscrowAPI.get_all_accessible_des()
    first_table_flag = True
    for de in accessible_de:
        if de.name == f"orders.csv":
            table_path = get_data(de)
            if first_table_flag:
                print("Creating o1")
                print(table_path)
                query = f"CREATE TABLE o1 AS SELECT * FROM read_csv({table_path}, " \
                        f"ignore_errors=1, auto_detect=1)"
                conn.execute(query)
                first_table_flag = False
            else:
                print("Creating o2")
                print(table_path)
                query = f"CREATE TABLE o2 AS SELECT * FROM read_csv({table_path}, " \
                        f"ignore_errors=1, auto_detect=1)"
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
def conclave_1():
    """SELECT COUNT(*) AS by_quantity FROM lineitem GROUPBY l_quantity ORDER BY l_quantity"""
    # Note: creating conn here, because we need to the same in-memory database
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    query = f"SELECT COUNT(*) AS by_quantity FROM lineitem GROUP BY l_quantity ORDER BY l_quantity"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def conclave_2():
    """SELECT (l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_quantity == 24"""
    # Note: creating conn here, because we need to the same in-memory database
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    query = f"SELECT (l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_quantity == 24"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def conclave_3():
    """SELECT * FROM orders o1 JOIN orders o2 ON o1.o_custkey = o2.o_custkey"""
    # Note: creating conn here, because we need to the same in-memory database
    conn = duckdb.connect()
    assemble_orders(conn)
    query = f"SELECT * FROM o1 JOIN o2 ON o1.o_custkey = o2.o_custkey"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def secyan_1():
    res = tpch_3()
    return res

@api_endpoint
@function
def secyan_2():
    res = tpch_10()
    return res

@api_endpoint
@function
def secyan_3():
    res = tpch_18()
    return res

@api_endpoint
@function
def secyan_4():
    res = tpch_8()
    return res

@api_endpoint
@function
def secyan_5():
    res = tpch_9()
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

@api_endpoint
@function
def tpch_9():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "part")
    assemble_table(conn, "supplier")
    assemble_table(conn, "partsupp")
    assemble_table(conn, "orders")
    assemble_table(conn, "nation")
    query = f"select nation, o_year, sum(amount) as sum_profit from " \
            f"( select n_name as nation, extract(year from o_orderdate) as o_year, " \
            f"l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount " \
            f"from part, supplier, lineitem, partsupp, orders, nation " \
            f"where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey " \
            f"and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey " \
            f"and p_name like '%aquamarine%' ) as profit " \
            f"group by nation, o_year order by nation, o_year desc"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_10():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "customer")
    assemble_table(conn, "orders")
    assemble_table(conn, "nation")
    query = f"select c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) as revenue, c_acctbal, " \
            f"n_name, c_address, c_phone, c_comment " \
            f"from customer, orders, lineitem, nation " \
            f"where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-03-01' " \
            f"and o_orderdate < date '1994-03-01' + interval '3' month and l_returnflag = 'R' " \
            f"and c_nationkey = n_nationkey " \
            f"group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment " \
            f"order by revenue desc limit 20"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_11():
    conn = duckdb.connect()
    assemble_table(conn, "partsupp")
    assemble_table(conn, "supplier")
    assemble_table(conn, "nation")
    query = f"select ps_partkey, sum(ps_supplycost * ps_availqty) as v " \
            f"from partsupp, supplier, nation " \
            f"where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'PERU' " \
            f"group by ps_partkey having sum(ps_supplycost * ps_availqty) > " \
            f"( select sum(ps_supplycost * ps_availqty) * 0.0001000000 from partsupp, supplier, nation " \
            f"where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'PERU' ) " \
            f"order by v desc"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_12():
    conn = duckdb.connect()
    assemble_table(conn, "orders")
    assemble_table(conn, "lineitem")
    query = f"select l_shipmode, " \
            f"sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) " \
            f"as high_line_count, " \
            f"sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) " \
            f"as low_line_count " \
            f"from orders, lineitem " \
            f"where o_orderkey = l_orderkey and l_shipmode in ('RAIL', 'SHIP') and l_commitdate < l_receiptdate " \
            f"and l_shipdate < l_commitdate and l_receiptdate >= date '1993-01-01' " \
            f"and l_receiptdate < date '1993-01-01' + interval '1' year " \
            f"group by l_shipmode order by l_shipmode"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_13():
    conn = duckdb.connect()
    assemble_table(conn, "customer")
    assemble_table(conn, "orders")
    query = f"select c_count, count(*) as custdist from " \
            f"( select c_custkey, count(o_orderkey) as c_count from customer left outer join orders " \
            f"on c_custkey = o_custkey and o_comment not like '%pending%deposits%' group by c_custkey ) as c_orders " \
            f"group by c_count order by custdist desc, c_count desc"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_14():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "part")
    query = f"select 100.00 * " \
            f"sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / " \
            f"sum(l_extendedprice * (1 - l_discount)) as promo_revenue " \
            f"from lineitem, part " \
            f"where l_partkey = p_partkey and l_shipdate >= date '1997-12-01' " \
            f"and l_shipdate < date '1997-12-01' + interval '1' month"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_15():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "supplier")
    query = f"with revenue_view as " \
            f"(select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue " \
            f"from lineitem where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01' group by l_suppkey) " \
            f"select s_suppkey, s_name, s_address, s_phone, total_revenue " \
            f"from supplier, revenue_view " \
            f"where s_suppkey = supplier_no and total_revenue = " \
            f"(select max(total_revenue) from revenue_view) " \
            f"order by s_suppkey"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_16():
    conn = duckdb.connect()
    assemble_table(conn, "partsupp")
    assemble_table(conn, "part")
    assemble_table(conn, "supplier")
    query = f"select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt " \
            f"from partsupp, part " \
            f"where p_partkey = ps_partkey and p_brand <> 'Brand#52' and p_type not like 'LARGE ANODIZED%' " \
            f"and p_size in (42, 38, 15, 48, 33, 3, 27, 45) and ps_suppkey not in " \
            f"( select s_suppkey from supplier where s_comment like '%Customer%Complaints%' ) " \
            f"group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_17():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "part")
    query = f"select sum(l_extendedprice) / 7.0 as avg_yearly " \
            f"from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'WRAP CASE' " \
            f"and l_quantity < " \
            f"( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey )"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_18():
    conn = duckdb.connect()
    assemble_table(conn, "customer")
    assemble_table(conn, "orders")
    assemble_table(conn, "lineitem")
    query = f"select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) " \
            f"from customer, orders, lineitem where o_orderkey in " \
            f"( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 314 ) " \
            f"and c_custkey = o_custkey and o_orderkey = l_orderkey " \
            f"group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice " \
            f"order by o_totalprice desc, o_orderdate limit 100"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_19():
    conn = duckdb.connect()
    assemble_table(conn, "lineitem")
    assemble_table(conn, "part")
    query = f"select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, part " \
            f"where ( p_partkey = l_partkey and p_brand = 'Brand#54' and p_container in " \
            f"('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 5 and l_quantity <= 5 + 10 and " \
            f"p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) " \
            f"or ( p_partkey = l_partkey and p_brand = 'Brand#25' and p_container in " \
            f"('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 19 and l_quantity <= 19 + 10 " \
            f"and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') " \
            f"and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey " \
            f"and p_brand = 'Brand#42' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') " \
            f"and l_quantity >= 24 and l_quantity <= 24 + 10 and p_size between 1 and 15 " \
            f"and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' )"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_20():
    conn = duckdb.connect()
    assemble_table(conn, "nation")
    assemble_table(conn, "supplier")
    assemble_table(conn, "partsupp")
    assemble_table(conn, "part")
    assemble_table(conn, "lineitem")
    query = f"select s_name, s_address from supplier, nation " \
            f"where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in " \
            f"( select p_partkey from part where p_name like 'purple%' ) and " \
            f"ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey " \
            f"and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' " \
            f"and l_shipdate < date '1994-01-01' + interval '1' year ) ) and s_nationkey = n_nationkey " \
            f"and n_name = 'UNITED KINGDOM' order by s_name"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_21():
    conn = duckdb.connect()
    assemble_table(conn, "supplier")
    assemble_table(conn, "lineitem")
    assemble_table(conn, "orders")
    assemble_table(conn, "nation")
    query = f"select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation " \
            f"where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' " \
            f"and l1.l_receiptdate > l1.l_commitdate and exists " \
            f"( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) " \
            f"and not exists " \
            f"( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey " \
            f"and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'FRANCE' " \
            f"group by s_name order by numwait desc, s_name limit 100"
    res = conn.execute(query).fetchall()
    return res

@api_endpoint
@function
def tpch_22():
    conn = duckdb.connect()
    assemble_table(conn, "customer")
    assemble_table(conn, "orders")
    query = f"select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from " \
            f"( select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer " \
            f"where substring(c_phone from 1 for 2) in ('33', '25', '16', '23', '32', '13', '19') " \
            f"and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 " \
            f"and substring(c_phone from 1 for 2) in ('33', '25', '16', '23', '32', '13', '19') ) and not exists " \
            f"( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode"
    res = conn.execute(query).fetchall()
    return res
