from dsapplicationregistration.dsar_core import api_endpoint, function
from contractapi.contract_api import ContractAPI

import csv
import pandas as pd
import duckdb
from scipy.stats import ks_2samp
from sklearn.linear_model import LogisticRegression
from sklearn import preprocessing

@api_endpoint
def upload_data_in_csv(de_in_bytes):
    return ContractAPI.CSVDEStore.write(de_in_bytes)

@api_endpoint
def propose_contract(dest_agents, des, f, *args, **kwargs):
    return ContractAPI.propose_contract(dest_agents, des, f, *args, **kwargs)

@api_endpoint
def approve_contract(contract_id):
    return ContractAPI.approve_contract(contract_id)

@api_endpoint
def upload_cmr(dest_a_id, de_id, f):
    return ContractAPI.upload_cmr(dest_a_id, de_id, f)

@api_endpoint
def show_contracts_pending_my_approval():
    return ContractAPI.show_contracts_pending_my_approval()

@api_endpoint
def show_my_contracts_pending_approval():
    return ContractAPI.show_my_contracts_pending_approval()

@api_endpoint
def list_all_des_with_src():
    return ContractAPI.list_all_des_with_src()

@api_endpoint
@function
def show_schema(de_ids):
    # import time
    """Return the schema of all DEs in contract, along with their IDs."""
    de_schema_dict = {}
    # Following line tests short-circuiting
    for de_id in de_ids:
        de_path = ContractAPI.CSVDEStore.read(de_id)
        with open(de_path, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)
            de_schema_dict[de_id] = header
    return de_schema_dict

@api_endpoint
@function
def share_sample(de_id, sample_size):
    de_path = ContractAPI.CSVDEStore.read(de_id)
    df = pd.read_csv(de_path)
    sample_size = min(sample_size, len(df))
    sample = df.sample(n=sample_size)
    return sample

@api_endpoint
@function
def share_de(de_id):
    de_path = ContractAPI.CSVDEStore.read(de_id)
    df = pd.read_csv(de_path)
    return df

@api_endpoint
@function
def check_column_compatibility(de_1, de_2, cols_1, cols_2):
    """
    Given two DEs, and two lists of column names from each DE, compare these columns 1 by 1.
    We return the result in a string.
    """
    # Loop through the cols array (needs to be same length)
    # For each pair: first get their types, then do KS test if both numerical, else jaccard similarity.
    res_str = ""
    de_1_path = ContractAPI.CSVDEStore.read(de_1)
    de_2_path = ContractAPI.CSVDEStore.read(de_2)
    conn = duckdb.connect()
    numerical_types = ['INT', 'BIGINT', 'SMALLINT', 'FLOAT', 'DOUBLE', 'DECIMAL']
    text_types = ['CHAR', 'VARCHAR', 'TEXT']
    date_time_types = ['DATE', 'TIME']
    boolean_types = ['BOOLEAN', 'BOOL']
    df_1 = conn.execute(f"SELECT * FROM read_csv_auto('{de_1_path}')").fetchdf()
    df_2 = conn.execute(f"SELECT * FROM read_csv_auto('{de_2_path}')").fetchdf()
    for i in range(len(cols_1)):
        col_1 = df_1[cols_1[i]]
        col_2 = df_2[cols_2[i]]
        type_1 = conn.execute(f"DESCRIBE SELECT {cols_1[i]} FROM read_csv_auto('{de_1_path}')").fetchall()[0][1]
        type_2 = conn.execute(f"DESCRIBE SELECT {cols_2[i]} FROM read_csv_auto('{de_2_path}')").fetchall()[0][1]
        res_str += f"{cols_1[i]} vs {cols_2[i]}: "
        print(cols_1[i])
        print(cols_2[i])
        # Case 1: both are numerical types: calculate their ks statistic
        if type_1 in numerical_types and type_2 in numerical_types:
            statistic, p_value = ks_2samp(col_1, col_2)
            if p_value > 0.05:
                res_str += "Both numerical types. The distributions are likely similar.   "
                print("Both numerical types. The distributions are likely similar.")
            else:
                res_str += "Both numerical types. The distributions are likely different.   "
                print("Both numerical types. The distributions are likely different.")
        # Case 2: both are text types: run jaccard similarity
        elif type_1 in text_types and type_2 in text_types:
            intersection_size = len(set(col_1).intersection(set(col_2)))
            union_size = len(set(col_1).union(set(col_2)))
            jaccard_sim = intersection_size / union_size if union_size != 0 else 0
            res_str += f"Both string/text types. Jaccard similarity is {jaccard_sim}.   "
            print(f"Both string/text types. Jaccard similarity is {jaccard_sim}.")
        elif type_1 in date_time_types and type_2 in date_time_types:
            res_str += "Both date/time types.   "
            print("Both date/time types.")
        else:
            res_str += "Date types are different.   "
            print("Date types are different.")
    return res_str

@api_endpoint
@function
def train_model_with_conditions(label_name,
                                train_de_ids,
                                test_de_ids,
                                size_constraint,
                                accuracy_constraint):
    """
    First combine all DEs in the contract. Then check for preconditions, train model, and check for postconditions.
    """
    train_df_list = []
    test_df_list = []
    for de_id in train_de_ids:
        de_path = ContractAPI.CSVDEStore.read(de_id)
        cur_df = pd.read_csv(de_path)
        if len(cur_df) < size_constraint:
            return "Pre-condition: input size constraint failed. Model did not train."
        train_df_list.append(cur_df)
    for de_id in test_de_ids:
        de_path = ContractAPI.CSVDEStore.read(de_id)
        cur_df = pd.read_csv(de_path)
        test_df_list.append(cur_df)
    train_df = pd.concat(train_df_list, ignore_index=True, axis=0)
    test_df = pd.concat(test_df_list, ignore_index=True, axis=0)
    X_train = train_df.drop(label_name, axis=1)
    scaler = preprocessing.StandardScaler().fit(X_train)
    y_train = train_df[label_name]
    X_train_scaled = scaler.transform(X_train)
    clf = LogisticRegression().fit(X_train_scaled, y_train)
    X_test = test_df.drop(label_name, axis=1)
    X_test_scaled = scaler.transform(X_test)
    y_test = test_df[label_name]
    if clf.score(X_test_scaled, y_test) < accuracy_constraint:
        return "Post-condition: accuracy requirement failed. Model cannot be released."
    print(clf.score(X_test_scaled, y_test))
    return clf
