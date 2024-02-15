from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import duckdb
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn import tree

@api_endpoint
def list_all_agents(user_id):
    return EscrowAPI.list_all_agents(user_id)

@api_endpoint
def register_de(user_id, de_name):
    return EscrowAPI.register_de(user_id, de_name, "file", de_name)

@api_endpoint
def upload_de(user_id, de_id, de_in_bytes):
    return EscrowAPI.upload_de(user_id, de_id, de_in_bytes)

@api_endpoint
def remove_de_from_storage(user_id, de_id):
    return EscrowAPI.remove_de_from_storage(user_id, de_id)

@api_endpoint
def remove_de_from_db(user_id, de_id):
    return EscrowAPI.remove_de_from_db(user_id, de_id)

@api_endpoint
def list_all_des_with_src(user_id):
    return EscrowAPI.list_all_des_with_src(user_id)

@api_endpoint
def get_all_functions(user_id):
    return EscrowAPI.get_all_functions(user_id)

@api_endpoint
def get_function_info(user_id, function_name):
    return EscrowAPI.get_function_info(user_id, function_name)

@api_endpoint
def propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs):
    return EscrowAPI.propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs)

@api_endpoint
def show_contract(user_id, contract_id):
    return EscrowAPI.show_contract(user_id, contract_id)

@api_endpoint
def show_all_contracts_as_dest(user_id):
    return EscrowAPI.show_all_contracts_as_dest(user_id)

@api_endpoint
def show_all_contracts_as_src(user_id):
    return EscrowAPI.show_all_contracts_as_src(user_id)

@api_endpoint
def approve_contract(user_id, contract_id):
    return EscrowAPI.approve_contract(user_id, contract_id)

@api_endpoint
def reject_contract(user_id, contract_id):
    return EscrowAPI.reject_contract(user_id, contract_id)

@api_endpoint
def execute_contract(user_id, contract_id):
    return EscrowAPI.execute_contract(user_id, contract_id)

def helper_for_SQL_query(query):
    accessible_des = EscrowAPI.get_all_accessible_des()
    facebook_path = None
    youtube_path = None
    for de in accessible_des:
        if de.id == 1:
            facebook_path = de.access_param
        else:
            youtube_path = de.access_param
    updated_query = query.replace(" de1 ", f" read_csv_auto('{facebook_path}') as de1 ", 1). \
        replace(" de2 ", f" read_csv_auto('{youtube_path}') as de2 ", 1)
    return updated_query

@function
def run_SQL_query(query):
    """
    Run a user given query
    """
    updated_query = helper_for_SQL_query(query)
    conn = duckdb.connect()
    res_df = conn.execute(updated_query).fetchdf()
    conn.close()
    return res_df

@function
def logistic_wrapper(query, label):
    res_df = run_SQL_query(query)
    X = res_df.drop(label, axis=1)
    y = res_df[label]
    clf = LogisticRegression().fit(X, y)
    return clf


# @function
# def train_ML_model(query, model_name, label_name):
#     if model_name not in ["logistic_regression", "linear_regression", "decision_tree"]:
#         return "Model Type not currently supported"
#     res_df = run_SQL_query(query)
#     X = res_df.drop(label_name, axis=1)
#     y = res_df[label_name]
#     if model_name == "logistic_regression":
#         clf = LogisticRegression().fit(X, y)
#     elif model_name == "linear_regression":
#         clf = LinearRegression().fit(X, y)
#     else:
#         clf = tree.DecisionTreeClassifier().fit(X, y)
#     return clf
