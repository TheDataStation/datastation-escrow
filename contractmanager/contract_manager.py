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

    # Check if this is a valid function
    function_res = database_api.get_all_functions()
    if function_res["status"] == 1:
        return function_res
    if function not in function_res["data"]:
        return {"status": 1, "message": "Contract function not valid"}

    # Add to the Contract table
    if write_ahead_log:
        wal_entry = f"database_api.create_contract({contract_id}, {function}, {param_str})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    db_res = database_api.create_contract(contract_id, function, param_str)
    if db_res["status"] == 1:
        return db_res

    # Add to ContractDest table and ContractDE table
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

    # Add to ContractStatus table. First get the src agents, default to DE owners.
    src_agent_de_dict = {}

    original_de_ids = get_original_des_from_het_des(data_elements)

    for de_id in original_de_ids:
        owner_id_res = database_api.get_de_owner_id(de_id)
        if owner_id_res["status"] == 1:
            return owner_id_res
        src_a_id = owner_id_res["data"]
        # If the key exists, append the value to its list
        if src_a_id in src_agent_de_dict:
            src_agent_de_dict[src_a_id].add(de_id)
        else:
            src_agent_de_dict.setdefault(src_a_id, set()).add(de_id)
    for src_a_id in src_agent_de_dict:
        if write_ahead_log:
            wal_entry = f"database_api.create_contract_status({contract_id}, {src_a_id}, 0)"
            write_ahead_log.log(user_id, wal_entry, key_manager, )
        db_res = database_api.create_contract_status(contract_id, src_a_id, 0)
        if db_res["status"] == 1:
            return db_res

    # Lastly, automatically invoke approve_contract for current caller, if caller is a source agent
    if user_id in src_agent_de_dict:
        approval_res = approve_contract(user_id, contract_id, write_ahead_log, key_manager)
        if approval_res["status"] == 1:
            return approval_res

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


def show_my_contracts_pending_approval(user_id):
    contract_ids_resp = database_api.get_all_contracts_for_dest(user_id)
    contract_objects = []
    if contract_ids_resp["status"] == 0:
        for c_id in contract_ids_resp["data"]:
            cur_contract = get_contract_object(c_id[0])
            if not cur_contract["ready_status"]:
                del cur_contract["ready_status"]
                contract_objects.append(cur_contract)
    if not len(contract_objects):
        return "There is no contracts pending approval whose output the current user will access."
    return contract_objects


def show_contracts_pending_my_approval(user_id):
    contract_ids_resp = database_api.get_contracts_pending_my_approval(user_id)
    contract_objects = []
    if contract_ids_resp["status"] == 0:
        for c_id in contract_ids_resp["data"]:
            cur_contract = get_contract_object(c_id[0])
            if not cur_contract["ready_status"]:
                del cur_contract["ready_status"]
                contract_objects.append(cur_contract)
    if not len(contract_objects):
        return "There is no contracts pending the current user's approval."
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

    approval_res = database_api.approve_contract(user_id, contract_id)
    if approval_res["status"] == 1:
        return approval_res
    # Whenever approve contract is called, we see if the associated contract is approved completely.
    # If True, add a list of policies to Policy table.
    contract_status = check_contract_ready(contract_id)
    if contract_status:
        dest_a_ids = get_dest_ids_for_contract(contract_id)
        de_ids = get_de_ids_for_contract(contract_id)
        function, function_param = get_contract_function_and_param(contract_id)
        de_ids.sort()
        de_ids = [str(de_id) for de_id in de_ids]
        de_ids_str = " ".join(de_ids)
        for a_id in dest_a_ids:
            if write_ahead_log:
                wal_entry = f"database_api.create_policy({a_id}, {de_ids_str}, {function}, {function_param})"
                write_ahead_log.log(user_id, wal_entry, key_manager, )
            print("In contract creation")
            print(a_id)
            print(de_ids_str)
            print(function)
            print(function_param)
            db_res = database_api.create_policy(a_id, de_ids_str, function, function_param)
            if db_res["status"] == 1:
                return db_res
    return approval_res

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
        if int(user_id) != owner_id_res["data"]:
            return {"status": 1, "message": "Upload CMP failure: Caller not owner of de"}

    if write_ahead_log:
        wal_entry = f"database_api.create_cmp({user_id}, {dest_a_id}, {de_id}, {function})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    return database_api.create_cmp(user_id, dest_a_id, de_id, function)


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
    function_param = contract_db_res["data"].function_param
    return function, function_param


def get_contract_object(contract_id):
    # Get the destination agents, the data elements, the template and its args, and the ready status
    des_in_contract = database_api.get_de_for_contract(contract_id)
    des_list = list(map(lambda ele: ele[0], des_in_contract))
    dest_agents = database_api.get_dest_for_contract(contract_id)
    dest_agents_list = list(map(lambda ele: ele[0], dest_agents))
    agents_db_resp = database_api.get_users_by_ids(dest_agents_list)
    dest_agents_info_list = []
    if agents_db_resp["status"] == 0:
        agents = agents_db_resp["data"]
        for agent in agents:
            dest_agents_info_list.append({"Username": agent.user_name, "User ID": agent.id})
    contract_db_res = database_api.get_contract(contract_id)
    function_param = json.loads(contract_db_res["data"].function_param)
    ready_status = check_contract_ready(contract_id)

    contract_obj = {
        "contract_id": contract_id,
        "who_can_access_contract_output": dest_agents_info_list,
        "data_needs_to_be_accessed": des_list,
        "function": contract_db_res["data"].function,
        "args": function_param["args"],
        "kwargs": function_param["kwargs"],
        "ready_status": ready_status,
    }
    return contract_obj


def get_original_des_from_het_des(het_de_ids):
    """
    Helper.
    Given a heterogeneous set of DEs (original or derived), return the original set
    """
    original_de_id_set = set()
    het_de_id_set = set()
    for het_de_id in het_de_ids:
        het_de_id_set.add(het_de_id)
    while het_de_id_set:
        cur_de_id = het_de_id_set.pop()
        cur_de = database_api.get_de_by_id(cur_de_id)["data"]
        # If it's an original DE, add it to result set
        if not cur_de.derived:
            original_de_id_set.add(cur_de_id)
        # If it's a derived DE: convert the DE to its source DEs, and add it back to het_de_id_set
        else:
            cur_src_des = database_api.get_source_des_for_derived_de(cur_de_id)
            des_to_add = set(map(lambda ele: ele[0], cur_src_des))
            het_de_id_set.update(des_to_add)
    # # print(het_de_id_set)
    # # print(original_de_id_set)
    return original_de_id_set

def check_release_status(dest_a_id, de_accessed, function, function_param):
    # Step 1: check if there's a policy existing: if yes, can release
    de_ids = list(get_original_des_from_het_des(de_accessed))
    de_ids.sort()
    de_ids = [str(de_id) for de_id in de_ids]
    de_ids_str = " ".join(de_ids)
    print("In check status")
    print(dest_a_id)
    print(de_ids_str)
    print(function)
    print(function_param)
    policy_exists = database_api.check_policy_exists(dest_a_id, de_ids_str, function, function_param)
    if policy_exists:
        return True

    # If a policy does not exist already, we apply CMRs and caller-auto-approval, and check again

    # Apply CMR:
    # For the source agents, we fetch all of their relevant CMPs, by function
    # Intuition: (0, 0) -> I approve of all my DEs in this contract for any dest agents, for this f()
    # Intuition: (dest_a_id, a_id) -> I approve of this DE, for this dest agent, for this f()

    # Step 1: Get what DEs in this contract are each src_agent are responsible for
    src_agent_de_dict = {}
    original_de_ids = get_original_des_from_het_des(de_accessed)
    # print("Check release status: Original DE IDs:", original_de_ids)
    for de_id in original_de_ids:
        owner_id_res = database_api.get_de_owner_id(de_id)
        if owner_id_res["status"] == 1:
            return owner_id_res
        src_a_id = owner_id_res["data"]
        # If the key exists, append the value to its list
        if src_a_id in src_agent_de_dict:
            src_agent_de_dict[src_a_id].add(de_id)
        else:
            src_agent_de_dict.setdefault(src_a_id, set()).add(de_id)
    print(src_agent_de_dict)

    # Step 2: Fetch relevant CMPs that each of these src agents have specified
    # We go over each src agent one by one

    src_agent_approval_dict = {}

    for src_a_id in src_agent_de_dict:
        de_accessible_to_all = set()
        cur_src_approval_set = set()
        cur_source_approved = False
        cur_cmrs = database_api.get_cmp_for_src_and_f(src_a_id, function)
        cur_cmrs = list(map(lambda ele: [ele.dest_a_id, ele.de_id], cur_cmrs))
        # print(cur_cmrs)
        for policy in cur_cmrs:
            # case 1: source agent auto-approves all DEs for all agents for this f()
            if policy == [0, 0]:
                cur_source_approved = True
                break
            # case 2: source agent auto-approves one DE for all agents for this f()
            elif policy[0] == 0:
                de_accessible_to_all.add(policy[1])
            # case 3: source agent auto-approves all DEs for this dest agent for this f()
            elif policy[0] == dest_a_id and policy[1] == 0:
                cur_src_approval_set = set(de_accessed)
            elif policy[0] == dest_a_id:
                cur_src_approval_set.add(policy[1])
        cur_src_approval_set.update(de_accessible_to_all)

        request_de_for_cur_src = original_de_ids.intersection(src_agent_de_dict[src_a_id])
        # print("Relevant request for current source agent is", request_de_for_cur_src)
        # print("Auto approved request for curret source agent is ", cur_src_approval_set)
        # We check if requested DE is a subset of approval set for the current source agent.
        # If true, set current source agent's approval status to True.
        if not cur_source_approved:
            if request_de_for_cur_src.issubset(cur_src_approval_set):
                cur_source_approved = True
        src_agent_approval_dict[src_a_id] = cur_source_approved

    # Apply caller auto-approval
    if dest_a_id in src_agent_de_dict:
        src_agent_approval_dict[dest_a_id] = True

    # print(src_agent_approval_dict)
    return all(value for value in src_agent_approval_dict.values())
