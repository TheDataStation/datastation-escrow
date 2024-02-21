import json

from dbservice import database_api


def propose_contract(user_id,
                     contract_id,
                     dest_agents,
                     data_elements,
                     function,
                     write_ahead_log,
                     key_manager,
                     *args,
                     **kwargs,
                     ):
    param_json = {"args": args, "kwargs": kwargs}
    param_str = json.dumps(param_json)

    # first check if this is a valid function
    function_res = database_api.get_all_functions()
    if function_res["status"] == 1:
        return function_res
    if function not in function_res["data"]:
        return {"status": 1, "message": "Contract function not valid"}

    # First add to the Contract table
    if write_ahead_log:
        wal_entry = f"database_api.create_contract({contract_id}, {function}, {param_str})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    db_res = database_api.create_contract(contract_id, function, param_str)
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

    # Then add to ContractStatus table. First get the src agents, default to DE owners.
    src_agent_set = set()

    simple_de_ides = get_simple_des_from_het_des(data_elements)

    for de_id in simple_de_ides:
        owner_id_res = database_api.get_de_owner_id(de_id)
        if owner_id_res["status"] == 1:
            return owner_id_res
        src_agent_set.add(owner_id_res["data"])
    for a_id in src_agent_set:
        if write_ahead_log:
            wal_entry = f"database_api.create_contract_status({contract_id}, {a_id}, 0)"
            write_ahead_log.log(user_id, wal_entry, key_manager, )
        db_res = database_api.create_contract_status(contract_id, a_id, 0)
        if db_res["status"] == 1:
            return db_res

    # Now: Apply CMPs to do auto-approval and auto-rejection
    # For the source agents, we fetch all of their relevant CMPs, by function
    # Intuition: (0, 0) -> I approve of all my DEs in this contract for any dest agents, for this f()
    # Intuition: (dest_a_id, a_id) -> I approve of this DE, for this dest agent, for this f()

    # Step 1: Get what DEs in this contract are each src_agent are responsible for


    # Step 2: Build their approval set

    # Step: Check that this contract is a subset of their approval set: if true, auto approves

    # Also, if caller is a src agent, they auto-approve

    return {"status": 0, "message": "success", "contract_id": contract_id}


def show_contract(user_id, contract_id):
    # Check if caller is contract's src or dest agent
    src_agents = database_api.get_src_for_contract(contract_id)
    dest_agents = database_api.get_dest_for_contract(contract_id)
    src_agents_list = list(map(lambda ele: ele[0], src_agents))
    dest_agents_list = list(map(lambda ele: ele[0], dest_agents))
    if int(user_id) not in src_agents_list and int(user_id) not in dest_agents_list:
        print("Caller not a src or dest agent of the contract.")
        return None

    return get_contract_object(contract_id)


def show_all_contracts_as_dest(user_id):
    contract_ids_resp = database_api.get_all_contracts_for_dest(user_id)
    contract_objects = []
    if contract_ids_resp["status"] == 0:
        for c_id in contract_ids_resp["data"]:
            cur_contract = get_contract_object(c_id[0])
            contract_objects.append(cur_contract)
    return contract_objects


def show_all_contracts_as_src(user_id):
    contract_ids_resp = database_api.get_all_contracts_for_src(user_id)
    contract_objects = []
    if contract_ids_resp["status"] == 0:
        for c_id in contract_ids_resp["data"]:
            cur_contract = get_contract_object(c_id[0])
            contract_objects.append(cur_contract)
    return contract_objects


def approve_contract(user_id,
                     contract_id,
                     write_ahead_log,
                     key_manager,
                     ):
    src_agents = database_api.get_src_for_contract(contract_id)
    src_agents_list = list(map(lambda ele: ele[0], src_agents))
    if int(user_id) not in src_agents_list:
        return {"status": 1, "message": "Caller not a source agent."}

    # If in no_trust mode, we need to record this in wal
    if write_ahead_log is not None:
        wal_entry = f"database_api.approve_contract({user_id}, {contract_id})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    return database_api.approve_contract(user_id, contract_id)


def reject_contract(user_id,
                    contract_id,
                    write_ahead_log,
                    key_manager,
                    ):
    src_agents = database_api.get_src_for_contract(contract_id)
    src_agents_list = list(map(lambda ele: ele[0], src_agents))
    if int(user_id) not in src_agents_list:
        return {"status": 1, "message": "Caller not a source agent."}

    # If in no_trust mode, we need to record this in wal
    if write_ahead_log is not None:
        wal_entry = f"database_api.reject_contract({user_id}, {contract_id})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    return database_api.reject_contract(user_id, contract_id)


def upload_cmp(user_id,
               dest_a_id,
               de_id,
               function,
               status,
               write_ahead_log,
               key_manager, ):
    # Check if this is a valid function
    function_res = database_api.get_all_functions()
    if function_res["status"] == 1:
        return function_res
    if function not in function_res["data"]:
        return {"status": 1, "message": "Function in CMP not valid"}

    # Check if caller is the owner of de_id
    if de_id:
        owner_id_res = database_api.get_de_owner_id(de_id)
        if owner_id_res["status"] == 1:
            return owner_id_res
        if user_id != owner_id_res["data"]:
            return {"status": 1, "message": "Upload CMP failure: Caller not owner of de"}

    if write_ahead_log:
        wal_entry = f"database_api.create_cmp({user_id}, {dest_a_id}, {de_id}, {function}, {status})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    return database_api.create_cmp(user_id, dest_a_id, de_id, function, status)


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

    function = contract_db_res["data"].function
    function_param = json.loads(contract_db_res["data"].function_param)
    return function, function_param


def get_contract_object(contract_id):
    # Get the destination agents, the data elements, the template and its args, and the ready status
    des_in_contract = database_api.get_de_for_contract(contract_id)
    des_list = list(map(lambda ele: ele[0], des_in_contract))
    dest_agents = database_api.get_dest_for_contract(contract_id)
    dest_agents_list = list(map(lambda ele: ele[0], dest_agents))
    contract_db_res = database_api.get_contract(contract_id)
    function_param = json.loads(contract_db_res["data"].function_param)
    ready_status = check_contract_ready(contract_id)

    contract_obj = {
        "a_dest": dest_agents_list,
        "des": des_list,
        "function": contract_db_res["data"].function,
        "args": function_param["args"],
        "kwargs": function_param["kwargs"],
        "ready_status": ready_status,
    }
    return contract_obj


def get_simple_des_from_het_des(het_de_ids):
    """
    Helper.
    Given a heterogeneous set of DEs (simple or intermediate), return a simple set
    """
    het_des = database_api.get_des_by_ids(het_de_ids)["data"]
    all_des = database_api.get_all_des()["data"]
    # print(het_des)
    simple_de_id_set = set()
    het_de_id_set = set()
    de_contract_dict = {}
    for de in het_des:
        het_de_id_set.add(de.id)
    for de in all_des:
        de_contract_dict[de.id] = de.contract_id
    while het_de_id_set:
        cur_de_id = het_de_id_set.pop()
        if de_contract_dict[cur_de_id] == 0:
            simple_de_id_set.add(cur_de_id)
        else:
            # print(de_contract_dict)
            # convert the intermediate DE into DEs from its contract
            des_in_contract = database_api.get_de_for_contract(de_contract_dict[cur_de_id])
            des_to_add = set(map(lambda ele: ele[0], des_in_contract))
            het_de_id_set.update(des_to_add)
    # print(het_de_id_set)
    # print(simple_de_id_set)
    return simple_de_id_set
