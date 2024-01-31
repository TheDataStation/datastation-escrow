from dsapplicationregistration.dsar_core import api_endpoint, function
from escrowapi.escrow_api import EscrowAPI

from dowhy import CausalModel
import duckdb
import networkx as nx

@api_endpoint
def list_all_agents(user_id):
    return EscrowAPI.list_all_agents(user_id)

@api_endpoint
def register_de(user_id, de_name, discoverable):
    return EscrowAPI.register_de(user_id, de_name, "file", de_name, discoverable)

@api_endpoint
def upload_de(user_id, de_id, de_in_bytes):
    return EscrowAPI.upload_de(user_id, de_id, de_in_bytes)

@api_endpoint
def remove_de_from_storage(user_id, de_id):
    return EscrowAPI.remove_de_from_storage(user_id, de_id)

@api_endpoint
def remove_de_from_db(user_id, de_id):
    return EscrowAPI.remove_de_from_db(user_id, de_id)

@api_endpoint
def list_discoverable_des_with_src(user_id):
    return EscrowAPI.list_discoverable_des_with_src(user_id)

@api_endpoint
def get_all_functions(user_id):
    return EscrowAPI.get_all_functions(user_id)

@api_endpoint
def get_function_info(user_id, function_name):
    return EscrowAPI.get_function_info(user_id, function_name)

@api_endpoint
def propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs):
    return EscrowAPI.propose_contract(user_id, dest_agents, data_elements, f, *args, **kwargs)

@api_endpoint
def show_contract(user_id, contract_id):
    return EscrowAPI.show_contract(user_id, contract_id)

@api_endpoint
def show_all_contracts_as_dest(user_id):
    return EscrowAPI.show_all_contracts_as_dest(user_id)

@api_endpoint
def show_all_contracts_as_src(user_id):
    return EscrowAPI.show_all_contracts_as_src(user_id)

@api_endpoint
def approve_contract(user_id, contract_id):
    return EscrowAPI.approve_contract(user_id, contract_id)

@api_endpoint
def execute_contract(user_id, contract_id):
    return EscrowAPI.execute_contract(user_id, contract_id)

@function
def calc_causal_dnpr(additional_vars, dag_spec, treatment, outcome):
    """
    Join input DE with additional_vars from DNPR DE. Then calculate treatment's effect on outcome
    :return: calculated effect (assuming linear)
    """
    des = EscrowAPI.get_all_accessible_des()
    dnpr_de_path = None
    input_de_path = None
    for de in des:
        if de.name == "DNPR.csv":
            dnpr_de_path = de.access_param
        else:
            input_de_path = de.access_param
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
