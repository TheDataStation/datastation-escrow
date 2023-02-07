from dsapplicationregistration.dsar_core import procedure, function
from common import escrow_api

import csv


@procedure
def upload_dataset(ds,
                   username,
                   data_name,
                   data_in_bytes,
                   data_type,
                   optimistic,
                   original_data_size=None):
    print("This is a customized upload data!")
    escrow_api.upload_dataset(ds, username, data_name, data_in_bytes, data_type, optimistic, original_data_size)

@procedure
def upload_policy(ds, username, policy):
    print("This is a customized upload policy!")
    escrow_api.upload_policy(ds, username, policy)

@function
def line_count():
    """count number of lines in a file"""
    print("starting counting line numbers")
    files = escrow_api.get_all_files()
    print(files)
    res = []
    for file in set(files):
        csv_file = open(file)
        reader = csv.reader(csv_file)
        res.append(len(list(reader)))
    return res
