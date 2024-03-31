from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

import csv
import pandas as pd
import duckdb
from scipy.stats import ks_2samp

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
def show_schema(de_ids):
    """Return the schema of all DEs in contract, along with their IDs."""
    de_schema_dict = {}
    for de_id in de_ids:
        de_path = EscrowAPI.CSVDEStore.read(de_id)
        with open(de_path, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)
            de_schema_dict[de_id] = header
    return de_schema_dict

@api_endpoint
@function
def share_sample(de_id, sample_size):
    de_path = EscrowAPI.CSVDEStore.read(de_id)
    df = pd.read_csv(de_path)
    sample_size = min(sample_size, len(df))
    sample = df.sample(n=sample_size)
    return sample

@api_endpoint
@function
def share_de(de_id):
    de_path = EscrowAPI.CSVDEStore.read(de_id)
    df = pd.read_csv(de_path)
    return df

@api_endpoint
@function
def check_column_compatibility(de_1, de_2, cols_1, cols_2):
    """
        Given two DEs, and two lists of column names from each DE, compare these columns 1 by 1.
        """
    # Loop through the cols array (needs to be same length)
    # For each pair: first get their types, then do KS test if both numerical, else jaccard similarity.
    de_1_path = EscrowAPI.CSVDEStore.read(de_1)
    de_2_path = EscrowAPI.CSVDEStore.read(de_2)
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
        print(cols_1[i])
        print(cols_2[i])
        # Case 1: both are numerical types: calculate their ks statistic
        if type_1 in numerical_types and type_2 in numerical_types:
            statistic, p_value = ks_2samp(col_1, col_2)
            if p_value > 0.05:
                print("Both numerical types. The distributions are likely similar.")
            else:
                print("Both numerical types. The distributions are likely different.")
        # Case 2: both are text types: run jaccard similarity
        elif type_1 in text_types and type_2 in text_types:
            intersection_size = len(set(col_1).intersection(set(col_2)))
            union_size = len(set(col_1).union(set(col_2)))
            jaccard_sim = intersection_size / union_size if union_size != 0 else 0
            print(f"Both string/text types. Jaccard similarity is {jaccard_sim}")
        elif type_1 in date_time_types and type_2 in date_time_types:
            print("Both date/time types.")
        else:
            print("Date types are different.")
