# Logistic regression model for the income dataset, full trust version.

import os.path
import pathlib
import numpy as np
import sys
from sklearn.linear_model import LogisticRegression

from dsapplicationregistration.dsar_core import register
from common import ds_utils

@register
def train_income_model():
    """train a logistic regression model on income data"""
    print("starting train model")
    files = ds_utils.get_all_files()
    Xtrains = []
    ytrains = []
    f_name_in_order = []
    for file in set(files):
        if pathlib.Path(file).is_file():
            f_name_in_order.append(file)
    f_name_in_order.sort(key=lambda x: int(x.split('/')[-2]))
    for file in f_name_in_order:
        array_to_append = file.split('/')[-1].split('.')[0][-1]
        cur_np_ele = np.load(file)
        if array_to_append == "X":
            Xtrains.append(cur_np_ele)
        else:
            ytrains.append(cur_np_ele)
    Xtrains = np.concatenate(Xtrains)
    ytrains = np.concatenate(ytrains)
    income_model = LogisticRegression()
    income_model.fit(Xtrains, ytrains)
    # return 5
    return income_model
