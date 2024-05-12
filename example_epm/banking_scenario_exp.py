from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import pandas as pd
from sklearn.neural_network import MLPClassifier
from sklearn import preprocessing

@api_endpoint
def upload_data_in_csv(de_in_bytes):
    return EscrowAPI.CSVDEStore.write(de_in_bytes)

@api_endpoint
def propose_contract(dest_agents, des, f, *args, **kwargs):
    return EscrowAPI.propose_contract(dest_agents, des, f, *args, **kwargs)

@api_endpoint
def approve_contract(contract_id):
    return EscrowAPI.approve_contract(contract_id)


@api_endpoint
@function
def train_model_with_conditions(label_name,
                                # train_de_ids,
                                # test_de_ids,
                                size_constraint,
                                accuracy_constraint):
    """
    First combine all DEs in the contract. Then check for preconditions, train model, and check for postconditions.
    """
    train_df_list = []
    test_df_list = []
    for de_id in [1, 3, 5, 7]:
        de_path = EscrowAPI.CSVDEStore.read(de_id)
        cur_df = pd.read_csv(de_path)
        if len(cur_df) < size_constraint:
            return "Pre-condition: input size constraint failed. Model did not train."
        train_df_list.append(cur_df)
    for de_id in [2, 4, 6, 8]:
        de_path = EscrowAPI.CSVDEStore.read(de_id)
        cur_df = pd.read_csv(de_path)
        test_df_list.append(cur_df)
    train_df = pd.concat(train_df_list, ignore_index=True, axis=0)
    test_df = pd.concat(test_df_list, ignore_index=True, axis=0)
    X_train = train_df.drop(label_name, axis=1)
    scaler = preprocessing.StandardScaler().fit(X_train)
    y_train = train_df[label_name]
    X_train_scaled = scaler.transform(X_train)
    clf = MLPClassifier()
    clf.fit(X_train_scaled, y_train)
    X_test = test_df.drop("is_fraud", axis=1)
    X_test_scaled = scaler.transform(X_test)
    y_test = test_df["is_fraud"]
    if clf.score(X_test_scaled, y_test) < accuracy_constraint:
        return "Post-condition: accuracy requirement failed. Model cannot be released."
    print(clf.score(X_test_scaled, y_test))
    return clf.coefs_
