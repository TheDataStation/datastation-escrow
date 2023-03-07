from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI
from typing import List, Tuple


@api_endpoint
def register_data(user_id,
                  data_name,
                  data_type,
                  access_param,
                  optimistic):
    print("This is a customized register data!")
    return EscrowAPI.register_data(user_id, data_name, data_type, access_param, optimistic)

@api_endpoint
def upload_data(user_id,
                data_id,
                data_in_bytes):
    return EscrowAPI.upload_data(user_id, data_id, data_in_bytes)

@api_endpoint
def upload_policy(user_id, user_id, api, data_id):
    print("This is a customized upload policy!")
    return EscrowAPI.upload_policy(user_id, user_id, api, data_id)

@api_endpoint
def suggest_share(user_id, agents, functions, data_elements):
    return EscrowAPI.suggest_share(user_id, agents, functions, data_elements)

@api_endpoint
def ack_data_in_share(user_id, data_id, share_id):
    return EscrowAPI.ack_data_in_share(user_id, data_id, share_id)

'''
returns all data elements registered in enclave mode with the data station
'''
@api_endpoint
@function
def show_data_in_ds(user_id) -> List[Tuple]:
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
def show_de_description(user_id, data_id) -> str:
    de = EscrowAPI.get_de_by_id(user_id, data_id)
    return de.desc

'''
Returns format for DE
'''
@api_endpoint
@function
def show_de_format(user_id, data_id) -> str:
    de = EscrowAPI.get_de_by_id(user_id, data_id)
    return de.type


@api_endpoint
@function
def request_de_schema(username, user_id, data_id): 
    return EscrowAPI.suggest_share(username, user_id, [user_id], ["show_de_schema"], [data_id])    

'''
Returns schema for DE
'''
# TODO: does this return anything?? what does suggest_share return!!!!! i dont
# know what any types areeeee
@api_endpoint
@function
def show_de_schema(user_id, data_id) -> List[Tuple[str, str]]:
    de = EscrowAPI.get_de_by_id(user_id, data_id)
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
@function
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
def column_intersection(user_id, data_id, attr_name: str, values: List = None,
                        my_data_id=None, my_attr_name: str = None) -> int:
    assert values is not None or (my_data_id is not None and my_attr_name is not None)
    # TODO assert both data_ids are present in the share?
    de = EscrowAPI.get_de_by_id(user_id, data_id)

    if values is not None:
        con = duckdb.connect()
        qry = f"SELECT COUNT(*) FROM {de.access_param} WHERE {attr_name} in {values}"
        results = con.execute(qry).fetchall() # results of the form: [(N,)]
        con.close()
        return results[0][0]
    else:
        myde = EscrowAPI.get_de_by_id(user_id, my_data_id)
        con = duckdb.connect()
        qry = f"SELECT COUNT(*) FROM {de.access_param} WHERE {attr_name} in {myde.access_param}.{my_attr_name}"
        results = con.execute(qry).fetchall() # results of the form: [(N,)]
        con.close()
        return results[0][0]

'''
Compare distributions between attributes within data elements
'''
@api_endpoint
@function
def compare_distributions(user_id, data_id, attr_name, my_data_id, my_attr_name):
    pass

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
def is_format_compatible(username, data_id, attr_name, my_data_id, my_attr_name):
    pass


'''
Request sample of data 
'''
@api_endpoint
@function
def request_sample(username, user_id, data_id)
    return EscrowAPI.suggest_share(username, [user_id], ["show_sample"],
                                   [data_id])

'''
Return sample of column from data element; default sample size is 10 rows
'''
@api_endpoint
@function
# NOTE: sample-size has to be fixed or this function could be abused. other
# situations where that can be the case?
def show_sample(username, data_id, attr_name):
    de = EscrowAPI.get_de_by_id(user_id, data_id)
    con = duckdb.connect()
    qry = "SELECT {attr_name} FROM {de.access_param} LIMIT 10"
    results = con.execute(qry).fetchall() # results of the form: [(N,)]
    con.close()
    return results



