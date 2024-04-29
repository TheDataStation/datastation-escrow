from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

from dowhy import CausalModel
import io
import duckdb
import networkx as nx
import pandas as pd


@api_endpoint
def upload_data_in_csv(de_in_bytes):
    # Check that user's uploaded data has an attribute called "CPR": the join key.
    df = pd.read_csv(io.BytesIO(de_in_bytes))
    if "CPR" in df.columns:
        return EscrowAPI.CSVDEStore.write(de_in_bytes, )
    else:
        return "Error: Input not CSV, or 'CPR' column does not exist in the data."


@api_endpoint
def approve_all_causal_queries():
    upload_cmp_res = EscrowAPI.upload_cmr(0, 1, "run_causal_query")
    if upload_cmp_res["status"] == 0:
        EscrowAPI.store("cmp_uploaded", True)
    return upload_cmp_res


@function
@api_endpoint
def run_causal_query(user_de_id, additional_vars, dag_spec, treatment, outcome):
    """
    Join input DE with additional_vars from DNPR's data. Then calculate treatment's effect on outcome, using user
    specified DAG.
    additional_vars: list of column names from DNPR's data to join with input DE.
    dag_spec: list of tuples (src, dst) specifying the causal DAG.
    treatment: name of treatment variable.
    outcome: name of outcome variable.
    :return: calculated effect (assuming linear) of treatment on outcome
    """
    dnpr_de_path = EscrowAPI.CSVDEStore.read(1)
    input_de_path = EscrowAPI.CSVDEStore.read(user_de_id)
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


# @api_endpoint
# def run_causal_query(user_id, user_DE_id, additional_vars, dag_spec, treatment, outcome):
#     cmp_uploaded = EscrowAPI.load("cmp_uploaded")
#     if cmp_uploaded:
#         # TODO: de_id = calc_causal_dnpr(params...)
#         proposition_res = EscrowAPI.propose_contract(user_id, [user_id], [1, user_DE_id], "calc_causal_dnpr",
#                                                      additional_vars, dag_spec, treatment, outcome)
#         return EscrowAPI.execute_contract(user_id, proposition_res["contract_id"])
#
#         # TODO: return calc_causal_dnpr(additional_vars, dag_spec, treatment, outcome)
#     else:
#         return "Wait for DNPR to approve all causal queries. Try again Later."
#         # when is destination agent a subset of the src agents
#
#
# @function
# def calc_causal_dnpr(additional_vars, dag_spec, treatment, outcome):
#     """
#     Join input DE with additional_vars from DNPR's data. Then calculate treatment's effect on outcome, using user
#     specified DAG.
#     additional_vars: list of column names from DNPR's data to join with input DE.
#     dag_spec: list of tuples (src, dst) specifying the causal DAG.
#     treatment: name of treatment variable.
#     outcome: name of outcome variable.
#     :return: calculated effect (assuming linear) of treatment on outcome
#     """
#     de_ids = EscrowAPI.get_contract_de_ids()
#     for de_id in de_ids:
#         if de_id == 1:
#             dnpr_de_path = EscrowAPI.CSVDEStore.read(de_id)
#         else:
#             input_de_path = EscrowAPI.CSVDEStore.read(de_id)
#     # First run the join
#     conn = duckdb.connect()
#     addtional_var_statement = ""
#     for var in additional_vars:
#         addtional_var_statement += f", table1.{var}"
#     query_statement = f"SELECT table2.*{addtional_var_statement} " \
#                       f"FROM read_csv_auto('{dnpr_de_path}') AS table1 " \
#                       f"JOIN read_csv_auto('{input_de_path}') AS table2 " \
#                       f"ON table1.CPR = table2.CPR;"
#     joined_df = conn.execute(query_statement).fetchdf()
#     conn.close()
#     # Now calculate the causal effect
#     causal_dag = nx.DiGraph(dag_spec)
#     model = CausalModel(
#         data=joined_df,
#         treatment=treatment,
#         outcome=outcome,
#         graph=causal_dag, )
#     identified_estimand = model.identify_effect(proceed_when_unidentifiable=True)
#     estimate = model.estimate_effect(identified_estimand,
#                                      method_name="backdoor.linear_regression")
#     return estimate.value
#
#     # return value from functions should always be treated as data elements
#     # if we decide that we need propose contracts, write down explicitly why having such API is beneficial
#     # what if we only want certain agents to call certain functions
#     # add authentication
#     # can we figure out which "types of agents" are supposed to call "what functions"?
#     # is it possible that destination agent is a subset of the src agent?
