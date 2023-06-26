import json

from dbservice import database_api
from common.pydantic_models.user import User
from common.pydantic_models.response import Response, UploadShareResponse


def register_share_in_DB(cur_username,
                         share_id,
                         dest_agents,
                         data_elements,
                         template,
                         *args,
                         **kwargs
                         ):
    # check if there is an existing user
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")

    # First add to the Share table
    param_json = {"args": args, "kwargs": kwargs}
    param_str = json.dumps(param_json)

    db_res = database_api.create_share(share_id, template, param_str)
    if db_res.status == -1:
        return Response(status=1, message="internal database error")

    # Then add to ShareDest table and ShareDE table
    for a_id in dest_agents:
        db_res = database_api.create_share_dest(share_id, a_id)
        if db_res.status == -1:
            return Response(status=1, message="internal database error")

    for de_id in data_elements:
        db_res = database_api.create_share_de(share_id, de_id)
        if db_res.status == -1:
            return Response(status=1, message="internal database error")

    # Then add to SharePolicy table. First get the approval agents, default to DE owners.
    approval_agent_set = set()
    for de_id in data_elements:
        owner_id = database_api.get_de_owner(de_id)
        approval_agent_set.add(owner_id)
    for a_id in approval_agent_set:
        db_res = database_api.create_share_policy(share_id, a_id, 0)
        if db_res.status == -1:
            return Response(status=1, message="internal database error")

    return UploadShareResponse(status=0, message="success", share_id=share_id)


def register_share_in_DB_no_trust(cur_username,
                                  share_id,
                                  dest_agents,
                                  data_elements,
                                  template,
                                  write_ahead_log,
                                  key_manager,
                                  *args,
                                  **kwargs,
                                  ):
    # check if there is an existing user
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    param_json = {"args": args, "kwargs": kwargs}
    param_str = json.dumps(param_json)

    # First add to the Share table
    wal_entry = f"database_api.create_share({share_id}, {template}, {param_str})"
    write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    db_res = database_api.create_share(share_id, template, param_str)
    if db_res.status == -1:
        return Response(status=1, message="internal database error")

    # Then add to ShareDest table and ShareDE table
    for a_id in dest_agents:
        wal_entry = f"database_api.create_share_dest({share_id}, {a_id})"
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )
        db_res = database_api.create_share_dest(share_id, a_id)
        if db_res.status == -1:
            return Response(status=1, message="internal database error")

    for de_id in data_elements:
        wal_entry = f"database_api.create_share_de({share_id}, {de_id})"
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )
        db_res = database_api.create_share_de(share_id, de_id)
        if db_res.status == -1:
            return Response(status=1, message="internal database error")

    # Then add to SharePolicy table. First get the approval agents, default to DE owners.
    approval_agent_set = set()
    for de_id in data_elements:
        owner_id = database_api.get_de_owner(de_id)
        approval_agent_set.add(owner_id)
    for a_id in approval_agent_set:
        wal_entry = f"database_api.create_share_policy({share_id}, {a_id}, 0)"
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )
        db_res = database_api.create_share_policy(share_id, a_id, 0)
        if db_res.status == -1:
            return Response(status=1, message="internal database error")

    return UploadShareResponse(status=0, message="success", share_id=share_id)

def show_share(cur_username, share_id):
    # First check if the caller is one of the approval agents
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    approval_agents = database_api.get_approval_for_share(share_id)
    approval_agents_list = list(map(lambda ele: ele[0], approval_agents))
    if cur_user_id not in approval_agents_list:
        return None

    # Get the destination agents, the data elements, the template and its args
    des_in_share = database_api.get_de_for_share(share_id)
    des_list = list(map(lambda ele: ele[0], des_in_share))
    dest_agents = database_api.get_dest_for_share(share_id)
    dest_agents_list = list(map(lambda ele: ele[0], dest_agents))
    share_db_res = database_api.get_share(share_id)

    share_param = json.loads(share_db_res.param)
    share_obj = {
        "a_dest": des_list,
        "des": dest_agents_list,
        "template": share_db_res.template,
        "args": share_param["args"],
        "kwargs": share_param["kwargs"],
    }

    return share_obj

def approve_share(cur_username,
                  share_id,
                  write_ahead_log=None,
                  key_manager=None,
                  ):
    # First check if the caller is one of the approval agents
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    approval_agents = database_api.get_approval_for_share(share_id)
    approval_agents_list = list(map(lambda ele: ele[0], approval_agents))
    if cur_user_id not in approval_agents_list:
        return None

    # If in no_trust mode, we need to record this in wal
    if write_ahead_log is not None:
        wal_entry = f"database_api.approve_share({cur_user_id}, {share_id})"
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    db_res = database_api.approve_share(cur_user_id, share_id)
    return db_res

def check_share_ready(share_id):
    db_res = database_api.get_status_for_share(share_id)
    status_list = list(map(lambda ele: ele[0], db_res))
    if 0 in status_list:
        return False
    return True

def get_de_ids_for_share(share_id):
    des_in_share = database_api.get_de_for_share(share_id)
    des_list = list(map(lambda ele: ele[0], des_in_share))
    return des_list
