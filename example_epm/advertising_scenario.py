from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import duckdb
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn import tree
import pickle

@api_endpoint
def list_all_agents(user_id):
    return EscrowAPI.list_all_agents(user_id)

@api_endpoint
def register_and_upload_de(user_id, de_name, data_in_bytes):
    register_res = EscrowAPI.register_de(user_id, de_name, "file", de_name)
    if register_res["status"] == 1:
        return register_res
    return EscrowAPI.upload_de(user_id, register_res["de_id"], data_in_bytes)

@api_endpoint
def list_all_agents(user_id):
    return EscrowAPI.list_all_agents(user_id)

@api_endpoint
def train_ML_model(user_id, data_elements, model_name, label_name, query=None):
    proposition_res = EscrowAPI.propose_contract(user_id, [user_id], data_elements, "train_ML_model", 1,
                                                 model_name, label_name, query)
    if proposition_res["contract_id"]:
        return EscrowAPI.execute_contract(user_id, proposition_res["contract_id"])
    return proposition_res

@api_endpoint
def propose_SQL_query(user_id, dest_agents, data_elements, query):
    return EscrowAPI.propose_contract(user_id, dest_agents, data_elements, "run_SQL_query", 0,
                                      query)

@api_endpoint
def approve_SQL_query(user_id, contract_id):
    return EscrowAPI.approve_contract(user_id, contract_id)

@api_endpoint
def execute_SQL_query(user_id, contract_id):
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
def train_ML_model(model_name, label_name, query=None):
    if model_name not in ["logistic_regression", "linear_regression", "decision_tree"]:
        return "Model Type not currently supported"
    if query:
        res_df = run_SQL_query(query)
    else:
        des = EscrowAPI.get_all_accessible_des()
        if len(des) != 1:
            print("Wrong number of DEs in contract")
            return -1
        else:
            with open(des[0].access_param, "rb") as f:
                res_df = pickle.load(f)
                print(res_df)
    X = res_df.drop(label_name, axis=1)
    y = res_df[label_name]
    if model_name == "logistic_regression":
        clf = LogisticRegression().fit(X, y)
    elif model_name == "linear_regression":
        clf = LinearRegression().fit(X, y)
    else:
        clf = tree.DecisionTreeClassifier().fit(X, y)
    return clf
