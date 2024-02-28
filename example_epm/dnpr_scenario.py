from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

from dowhy import CausalModel
import io
import duckdb
import networkx as nx
import pandas as pd


@api_endpoint
def register_and_upload_de(user_id, de_in_bytes):
    # Check that user's uploaded data has an attribute called "CPR": the join key.
    df = pd.read_csv(io.BytesIO(de_in_bytes))
    if "CPR" in df.columns:
        return EscrowAPI.CSVDEStore.write(user_id, de_in_bytes, )
    else:
        return "Error: Input not CSV, or 'CPR' does not exist in the data."


@api_endpoint
def upload_cmp(user_id, dest_a_id, de_id, f):
    EscrowAPI.store_kv_to_app_state("cmp_uploaded", True)
    return EscrowAPI.upload_cmp(user_id, dest_a_id, de_id, f)


@api_endpoint
def run_causal_query(user_id, user_DE_id, additional_vars, dag_spec, treatment, outcome):
    proposition_res = EscrowAPI.propose_contract(user_id, [user_id], [1, user_DE_id], "calc_causal_dnpr",
                                                 additional_vars, dag_spec, treatment, outcome)
    cmp_uploaded = EscrowAPI.load_key_from_app_state("cmp_uploaded")
    if cmp_uploaded:
        print("Note: Existing contract management policy in place")
    if proposition_res["contract_id"]:
        return EscrowAPI.execute_contract(user_id, proposition_res["contract_id"])
    else:
        return "Function did not execute successfully."


@function
def calc_causal_dnpr(additional_vars, dag_spec, treatment, outcome):
    """
    Join input DE with additional_vars from DNPR's data. Then calculate treatment's effect on outcome, using user
    specified DAG.
    additional_vars: list of column names from DNPR's data to join with input DE.
    dag_spec: list of tuples (src, dst) specifying the causal DAG.
    treatment: name of treatment variable.
    outcome: name of outcome variable.
    :return: calculated effect (assuming linear) of treatment on outcome
    """
    de_ids = EscrowAPI.get_contract_de_ids()
    for de_id in de_ids:
        if de_id == 1:
            dnpr_de_path = EscrowAPI.CSVDEStore.read(de_id)
        else:
            input_de_path = EscrowAPI.CSVDEStore.read(de_id)
    # First run the join
    conn = duckdb.connect()
    addtional_var_statement = ""
    for var in additional_vars:
        addtional_var_statement += f", table1.{var}"
    query_statement = f"SELECT table2.*{addtional_var_statement} " \
                      f"FROM read_csv_auto('{dnpr_de_path}') AS table1 " \
                      f"JOIN read_csv_auto('{input_de_path}') AS table2 " \
                      f"ON table1.CPR = table2.CPR;"
    joined_df = conn.execute(query_statement).fetchdf()
    conn.close()
    # Now calculate the causal effect
    causal_dag = nx.DiGraph(dag_spec)
    model = CausalModel(
        data=joined_df,
        treatment=treatment,
        outcome=outcome,
        graph=causal_dag, )
    identified_estimand = model.identify_effect(proceed_when_unidentifiable=True)
    estimate = model.estimate_effect(identified_estimand,
                                     method_name="backdoor.linear_regression")
    return estimate.value
