# Logistic regression model for the income dataset, no trust version.

import pathlib
import numpy as np
from sklearn.linear_model import LogisticRegression
import pickle

from dsapplicationregistration.dsar_core import register
from contractapi import contract_api


@register
def train_income_model():
    """train a logistic regression model on income data"""
    print("starting train model")
    files = escrow_api.get_all_files()
    Xtrains = []
    ytrains = []
    f_name_in_order = []
    for file in set(files):
        if pathlib.Path(file).is_file():
            f_name_in_order.append(file)
    f_name_in_order.sort(key=lambda x: int(x.split('/')[-2]))
    print(f_name_in_order)
    for file in f_name_in_order:
        f = open(file, "rb")
        cur_file_bytes = f.read()
        f.close()
        print(len(cur_file_bytes))
        print(cur_file_bytes[:100])
        # These bytes should be decrpted already, we convert it to object
        cur_np_ele = pickle.loads(cur_file_bytes)
        array_to_append = file.split('/')[-1].split('.')[0][-1]
        if array_to_append == "X":
            Xtrains.append(cur_np_ele)
        else:
            ytrains.append(cur_np_ele)
    Xtrains = np.concatenate(Xtrains)
    ytrains = np.concatenate(ytrains)
    income_model = LogisticRegression()
    income_model.fit(Xtrains, ytrains)
    return income_model
