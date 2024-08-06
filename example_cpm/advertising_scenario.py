from dsapplicationregistration.dsar_core import api_endpoint, function
from contractapi.contract_api import ContractAPI

import duckdb
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn import tree
import pickle

@api_endpoint
def list_all_agents():
    return ContractAPI.list_all_agents()

@api_endpoint
def list_all_des_with_src():
    return ContractAPI.list_all_des_with_src()

@api_endpoint
def get_all_functions():
    return ContractAPI.get_all_functions()

@api_endpoint
def get_function_info(function_name):
    return ContractAPI.get_function_info(function_name)

@api_endpoint
def upload_data_in_csv(content):
    return ContractAPI.CSVDEStore.write(content)

@api_endpoint
def propose_contract(dest_agents, des, f, *args, **kwargs):
    return ContractAPI.propose_contract(dest_agents, des, f, *args, **kwargs)

@api_endpoint
def show_all_contracts_as_src():
    return ContractAPI.show_all_contracts_as_src()

@api_endpoint
def list_all_des_with_src():
    return ContractAPI.list_all_des_with_src()

@api_endpoint
def approve_contract(contract_id):
    return ContractAPI.approve_contract(contract_id)


# What should the income query look like? We will do a query replacement
# select * from facebook
# select facebook.firstname from facebook

# update_query will do replace("facebook") with read_csv_auto("path_to_facebook") as facebook
# (and similarly for YouTube)
def update_query(query):
    from_index = query.lower().find("from")
    query_first_half = query[:from_index]
    query_second_half = query[from_index:]
    facebook_accessed = query.lower().find("facebook")
    youtube_accessed = query.lower().find("youtube")
    if facebook_accessed != -1:
        facebook_path = ContractAPI.CSVDEStore.read(1)
        query_second_half = query_second_half.replace("facebook", f"read_csv_auto('{facebook_path}') as facebook", 1)
    if youtube_accessed != -1:
        youtube_path = ContractAPI.CSVDEStore.read(2)
        query_second_half = query_second_half.replace("youtube", f"read_csv_auto('{youtube_path}') as youtube", 1)
    return query_first_half + query_second_half


@api_endpoint
@function
def run_query(query):
    """
    Run a user given query.
    """
    updated_query = update_query(query)
    conn = duckdb.connect()
    res_df = conn.execute(updated_query).fetchdf()
    conn.close()
    return res_df


@api_endpoint
@function
def train_model_over_joined_data(model_name, label_name, query=None):
    if model_name not in ["logistic_regression", "linear_regression", "decision_tree"]:
        return "Model Type not currently supported"
    # First check if the joined df has been preserved already
    joined_de_id = ContractAPI.load("joined_de_id")
    res_df = None
    if joined_de_id:
        print(joined_de_id)
        res_df = ContractAPI.ObjectDEStore.read(joined_de_id)
    elif query:
        res_df = run_query(query)
        joined_de_id = ContractAPI.ObjectDEStore.write(res_df)
        ContractAPI.store("joined_de_id", joined_de_id["de_id"])
    if res_df is not None:
        X = res_df.drop(label_name, axis=1)
        y = res_df[label_name]
        if model_name == "logistic_regression":
            clf = LogisticRegression().fit(X, y)
        elif model_name == "linear_regression":
            clf = LinearRegression().fit(X, y)
        else:
            clf = tree.DecisionTreeClassifier().fit(X, y)
        return clf
    else:
        return "There is no saved joined result or query to save data. Cannot train model."







