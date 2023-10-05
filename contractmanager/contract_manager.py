import json

from dbservice import database_api


def register_contract_in_DB(user_id,
                            contract_id,
                            dest_agents,
                            data_elements,
                            template,
                            write_ahead_log,
                            key_manager,
                            *args,
                            **kwargs,
                            ):
    param_json = {"args": args, "kwargs": kwargs}
    param_str = json.dumps(param_json)

    # First add to the Contract table
    if write_ahead_log:
        wal_entry = f"database_api.create_contract({contract_id}, {template}, {param_str})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    db_res = database_api.create_contract(contract_id, template, param_str)
    if db_res["status"] == 1:
        return db_res

    # Then add to ContractDest table and ContractDE table
    for a_id in dest_agents:
        if write_ahead_log:
            wal_entry = f"database_api.create_contract_dest({contract_id}, {a_id})"
            write_ahead_log.log(user_id, wal_entry, key_manager, )
        db_res = database_api.create_contract_dest(contract_id, a_id)
        if db_res["status"] == 1:
            return db_res

    for de_id in data_elements:
        if write_ahead_log:
            wal_entry = f"database_api.create_contract_de({contract_id}, {de_id})"
            write_ahead_log.log(user_id, wal_entry, key_manager, )
        db_res = database_api.create_contract_de(contract_id, de_id)
        if db_res["status"] == 1:
            return db_res

    # Then add to ContractStatus table. First get the approval agents, default to DE owners.
    approval_agent_set = set()
    for de_id in data_elements:
        owner_id_res = database_api.get_de_owner_id(de_id)
        if owner_id_res["status"] == 1:
            return owner_id_res
        approval_agent_set.add(owner_id_res["data"])
    for a_id in approval_agent_set:
        if write_ahead_log:
            wal_entry = f"database_api.create_contract_status({contract_id}, {a_id}, 0)"
            write_ahead_log.log(user_id, wal_entry, key_manager, )
        db_res = database_api.create_contract_status(contract_id, a_id, 0)
        if db_res["status"] == 1:
            return db_res

    return {"status": 0, "message": "success", "contract_id": contract_id}


def show_contract(user_id, contract_id):
    # Check if caller is contract's approval agent
    approval_agents = database_api.get_approval_for_contract(contract_id)
    approval_agents_list = list(map(lambda ele: ele[0], approval_agents))
    if int(user_id) not in approval_agents_list:
        print("Caller not an approval agent of the contract.")
        return None

    # Get the destination agents, the data elements, the template and its args
    des_in_contract = database_api.get_de_for_contract(contract_id)
    des_list = list(map(lambda ele: ele[0], des_in_contract))
    dest_agents = database_api.get_dest_for_contract(contract_id)
    dest_agents_list = list(map(lambda ele: ele[0], dest_agents))
    contract_db_res = database_api.get_contract(contract_id)

    function_param = json.loads(contract_db_res["data"].function_param)
    contract_obj = {
        "a_dest": dest_agents_list,
        "des": des_list,
        "function": contract_db_res["data"].function,
        "args": function_param["args"],
        "kwargs": function_param["kwargs"],
    }

    return contract_obj


def approve_contract(user_id,
                     contract_id,
                     write_ahead_log,
                     key_manager,
                     ):
    approval_agents = database_api.get_approval_for_contract(contract_id)
    approval_agents_list = list(map(lambda ele: ele[0], approval_agents))
    if int(user_id) not in approval_agents_list:
        return {"status": 1, "message": "Caller not an approval agent."}

    # If in no_trust mode, we need to record this in wal
    if write_ahead_log is not None:
        wal_entry = f"database_api.approve_contract({user_id}, {contract_id})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    return database_api.approve_contract(user_id, contract_id)


def check_contract_ready(contract_id):
    db_res = database_api.get_status_for_contract(contract_id)
    status_list = list(map(lambda ele: ele[0], db_res))
    if 0 in status_list:
        return False
    return True


def get_de_ids_for_contract(contract_id):
    des_in_contract = database_api.get_de_for_contract(contract_id)
    des_list = list(map(lambda ele: ele[0], des_in_contract))
    return des_list


def get_dest_ids_for_contract(contract_id):
    dest_agents = database_api.get_dest_for_contract(contract_id)
    dest_agents_list = list(map(lambda ele: ele[0], dest_agents))
    return dest_agents_list


def get_contract_function_and_param(contract_id):
    contract_db_res = database_api.get_contract(contract_id)
    print(contract_db_res)
    function = contract_db_res["data"].function
    function_param = json.loads(contract_db_res["data"].function_param)
    return function, function_param
