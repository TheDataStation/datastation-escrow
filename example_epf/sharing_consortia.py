from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

@api_endpoint
def register_de(user_id, data_name, data_type, access_param, optimistic):
    return EscrowAPI.register_de(user_id, data_name, data_type, access_param, optimistic)

@api_endpoint
def upload_de(user_id, data_id, data_in_bytes):
    return EscrowAPI.upload_de(user_id, data_id, data_in_bytes)

@api_endpoint
def list_discoverable_des(user_id):
    return EscrowAPI.list_discoverable_des(user_id)

@api_endpoint
def suggest_share(user_id, dest_agents, data_elements, template, *args, **kwargs):
    return EscrowAPI.suggest_share(user_id, dest_agents, data_elements, template, *args, **kwargs)

@api_endpoint
def show_share(user_id, share_id):
    return EscrowAPI.show_share(user_id, share_id)

@api_endpoint
def approve_share(user_id, share_id):
    return EscrowAPI.approve_share(user_id, share_id)

@api_endpoint
def execute_share(user_id, share_id):
    return EscrowAPI.execute_share(user_id, share_id)

@api_endpoint
def release_staged(user_id):
    return EscrowAPI.release_staged(user_id)

def setup_ti_with_di(di):
    """
    Sets up agent i's task ti with his own data di
    """
    data = pd.read_csv(di)
    data = data.drop(["PassengerId", "Name", "Ticket", "Cabin"], axis=1)
    data = data.dropna()
    label_encoder = LabelEncoder()
    data["Sex"] = label_encoder.fit_transform(data["Sex"])
    data["Embarked"] = label_encoder.fit_transform(data["Embarked"])

    X_train = data.drop("Survived", axis=1)
    y_train = data["Survived"]
    clf = RandomForestClassifier(random_state=1)
    clf.fit(X_train, y_train)
    return clf

def setup_ti_with_d_combined():
    df_list = []
    des = EscrowAPI.get_all_accessible_des()
    for de in des:
        de_path = de.access_param
        cur_df = pd.read_csv(de_path)
        df_list.append(cur_df)

    data = pd.concat(df_list, ignore_index=True)
    data = data.drop(["PassengerId", "Name", "Ticket", "Cabin"], axis=1)
    data = data.dropna()
    label_encoder = LabelEncoder()
    data["Sex"] = label_encoder.fit_transform(data["Sex"])
    data["Embarked"] = label_encoder.fit_transform(data["Embarked"])
    X_train = data.drop("Survived", axis=1)
    y_train = data["Survived"]
    clf = RandomForestClassifier(random_state=1)
    clf.fit(X_train, y_train)
    return clf

def evaluate_bi_with_ti(ti, bi):
    """
    Evaluate agent i's ti using his oen benchmark bi
    """
    data = pd.read_csv(bi)
    data = data.drop(["PassengerId", "Name", "Ticket", "Cabin"], axis=1)
    data = data.dropna()
    label_encoder = LabelEncoder()
    data["Sex"] = label_encoder.fit_transform(data["Sex"])
    data["Embarked"] = label_encoder.fit_transform(data["Embarked"])
    X_test = data.drop("Survived", axis=1)
    y_test = data["Survived"]
    y_pred = ti.predict(X_test)
    return accuracy_score(y_test, y_pred)

@api_endpoint
@function
def calc_pi_and_pip():
    clf_combined = setup_ti_with_d_combined()
    pi_list = []
    pip_list = []
    di_list = []
    bi_list = []
    des = EscrowAPI.get_all_accessible_des()
    for de in des:
        de_path = de.access_param
        if de_path[-6] == "d":
            di_list.append(de_path)
        else:
            bi_list.append(de_path)
    di_list = sorted(di_list, key=lambda x: int(x[-5]))
    bi_list = sorted(bi_list, key=lambda x: int(x[-5]))
    for i in range(len(di_list)):
        cur_clf = setup_ti_with_di(di_list[i])
        cur_pi = evaluate_bi_with_ti(cur_clf, bi_list[i])
        cur_pip = evaluate_bi_with_ti(clf_combined, bi_list[i])
        pi_list.append(cur_pi)
        pip_list.append(cur_pip)
    for i in range(1, len(di_list)+1):
        EscrowAPI.write_staged(f"p{i}_p{i}p.txt", i, [pi_list[i-1], pip_list[i-1]])
    return pi_list, pip_list
