from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI
from typing import List, Tuple
import duckdb
import numpy as np
from scipy.stats import mannwhitneyu
import csv

def get_data(de):
    if de.type == "file":
        return f"'{de.access_param}'"

def get_column(de_id, attr_name) -> List:
    de = EscrowAPI.get_de_by_id(de_id)
    table_name = get_data(de)
    query = f"SELECT {attr_name} FROM {table_name}"
    res = duckdb.sql(query).fetchall()
    return res

def cast_colummn(de_id, attr_name, cast_type) -> List:
    de = EscrowAPI.get_de_by_id(de_id)
    table_name = get_data(de)
    query = f"SELECT CAST({attr_name} AS {cast_type}) FROM {table_name}"
    try:
        res = duckdb.sql(query).fetchall()
        return res
    except duckdb.ConversionException:
        # cast was not possible
        return None

def get_column_type(de_id, attr_name) -> str:
    de = EscrowAPI.get_de_by_id(de_id)
    table_name = get_data(de)
    con = duckdb.connect()
    qry = f"DESCRIBE SELECT {attr_name} FROM '{de.access_param}'"
    schemas = con.execute(qry).fetchall()
    assert len(schemas) == 1
    con.close()
    return schemas[0][0][1]

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

'''
returns all data elements registered in enclave mode with the data station
'''
@api_endpoint
@function
def show_data_in_ds() -> List[Tuple]:
    data_elements = EscrowAPI.get_all_accessible_des()
    ret: List[Tuple] = []
    for de in data_elements:
        ret.append((de.id, de.owner_id))
    return ret

'''
Returns description for DE provided during register
'''
@api_endpoint
@function
def show_de_description(de_id) -> str:
    de = EscrowAPI.get_de_by_id(de_id)
    return de.desc

'''
Returns format for DE
'''
@api_endpoint
@function
def show_de_format(de_id) -> str:
    de = EscrowAPI.get_de_by_id(de_id)
    return de.type


@api_endpoint
def request_de_schema(username, user_id, data_id): 
    return EscrowAPI.suggest_share(username, user_id, [user_id], ["show_de_schema"], [data_id])    

'''
Returns schema for DE
'''
@api_endpoint
@function
def show_de_schema(de_id) -> List[Tuple[str, str]]:
    de = EscrowAPI.get_de_by_id(de_id)
    if de.type == "file":
        con = duckdb.connect()
        qry = f"DESCRIBE SELECT * FROM '{de.access_param}'"
        schemas = con.execute(qry).fetchall()
        ret: List[Tuple[str, str]] = []
        for s in schemas:
            t = (s[0], s[1])
            ret.append(t)
        con.close()
        return ret
    else:
        raise NotImplementedError(f"Attempting to read schema from unsupported data type: {de.type}")

'''
Create share to run column_intersection. Add your data in if you want to use it
in the function execution
'''
@api_endpoint
def request_column_intersection(username, user_id, data_id, my_data_id=None):
    data_elements = [data_id]
    if my_data_id is not None:
        data_elements.append(my_data_id)
    return EscrowAPI.suggest_share(username, [user_id], ["column_intersection"],
                                   data_elements)

'''
Performs private set intersection between values in column attr_name in DE
data_id and either the contents of the `values` List or the contents of column
my_attr_name in DE my_data_id
'''
@api_endpoint
@function
def column_intersection(src_de_id, src_attr: str, values: List = None, 
                                   tgt_de_id=None, tgt_attr: str = None) -> int:
    assert values is not None or (tgt_de_id is not None and tgt_attr is not None)
    src_col = get_column(src_de_id, src_attr)
    if values is not None:
        overlapping_values = set(src_col).intersection(set(values))
        return len(overlapping_values)
    else:
        tgt_col = get_column(tgt_de_id, tgt_attr)
        overlapping_values = set(src_col).intersection(set(tgt_col))
        return len(overlapping_values)

@api_endpoint
def request_compatibility_check(username, user_id, src_de_id, tgt_de_id):
    return EscrowAPI.suggest_share(username, [user_id], ["is_format_compatible"],
                                   [src_de_id, tgt_de_id])

@api_endpoint
@function
def is_format_compatible(src_de_id, src_attr, tgt_de_id, tgt_attr) -> Tuple[bool, str]:
    # try cast one to the other
    # try cast other to one
    # if so, compare distrib, pix, use basic guidelines ( >= smaller relation?)
    src_type = get_column_type(src_de_id, src_attr)
    tgt_type = get_column_type(tgt_de_id, tgt_attr)

    src_col, tgt_col = None, None
    src_cast = cast_colummn(src_de_id, src_attr, tgt_type)
    if src_cast is None:
        tgt_cast = cast_colummn(tgt_de_id, tgt_attr, src_type)
        if tgt_cast is None:
            return False, "Columns cannot be made to be the same type"
        else:
            src_col = get_column(src_de_id, src_attr)
            tgt_col = tgt_cast
    else:
        src_col = src_cast
        tgt_col = get_column(tgt_de_id, tgt_attr)

    smaller_column_size = min(len(src_col), len(tgt_col))
    U1, p = mannwhitneyu(src_col, tgt_col) 
    len_intersection = len(set(src_col).intersection(set(tgt_col)))
    if p < 0.05:
        if len_intersection >= 0.5 * smaller_column_size:
            return True, "Similar formats, distributions, and large intersection of values"
        else:
            return False, "Similar formats, distributions, but intersection is small"
    else:
        if len_intersection >= 0.5 * smaller_column_size:
            return False, "Similar formats, different distributions, but large intersection of values"
        else:
            return False, "Similar formats, but different distributions and small intersection"
    
'''
Suggest that the owner of `data_id` reformat the values in `attr_name` to match
the format provided by `fmt`
'''
@api_endpoint
@function
def suggest_data_format(user_id, data_id, attr_name, fmt: str):
    pass

@api_endpoint
@function
def reformat_data(user_id, de_id, attr_name, fmt: str):
    pass


@api_endpoint
def request_compare_distrib(username, user_id, src_data_id, tgt_data_id):
    return EscrowAPI.suggest_share(username, [user_id], ["compare_distributions"],
                                   [src_data_id, tgt_data_id])

'''
Compare distributions between attributes within data elements
'''
@api_endpoint
@function
def compare_distributions(src_de_id, src_attr, tgt_de_id, tgt_attr) -> Tuple:
    src_col = get_column(src_de_id, src_attr)
    tgt_col = get_column(tgt_de_id, tgt_attr)
    U1, p = mannwhitneyu(src_col, tgt_col)
    if p < 0.05:
        return False
    else:
        return True
    # return U1, p, np.mean(src_col), np.std(src_col), np.mean(tgt_col), np.std(tgt_col)

'''
Request sample of data 
'''
@api_endpoint
def request_sample(username, user_id, data_id):
    return EscrowAPI.suggest_share(username, [user_id], ["show_sample"],
                                   [data_id])

'''
Return sample of column from data element; default sample size is 10 rows
'''
@api_endpoint
@function
# NOTE: sample-size has to be fixed or this function could be abused. 
# are there other situations where that can be the case?
def show_sample(de_id, attr_name):
    de = EscrowAPI.get_de_by_id(de_id)
    con = duckdb.connect()
    qry = "SELECT {attr_name} FROM {de.access_param} LIMIT 10"
    results = con.execute(qry).fetchall() # results of the form: [(N,)]
    con.close()
    return results
