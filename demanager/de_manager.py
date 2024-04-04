import pathlib

from dbservice import database_api
from common import common_procedure


def register_de_in_DB(de_id,
                      user_id,
                      store_type,
                      derived,
                      write_ahead_log,
                      key_manager, ):
    # Call DB to register a new data element in the database

    # if pathlib.Path(str(access_param)).is_file():
    #     access_param = str(pathlib.Path(str(access_param)).absolute())

    # If in no_trust mode, we need to record this ADD_DATA to wal
    if write_ahead_log:
        wal_entry = f"database_api.create_de({de_id}, {user_id}, {store_type}, {derived})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    if derived:
        user_id = 0

    de_resp = database_api.create_de(int(de_id),
                                     int(user_id),
                                     store_type,
                                     derived)
    if de_resp["status"] == 1:
        return de_resp

    return {"status": de_resp["status"], "message": de_resp["message"], "de_id": de_resp["data"].id}


def remove_de_from_db(user_id,
                      de_id,
                      write_ahead_log,
                      key_manager, ):

    # Check that DE exists in database
    de_resp = database_api.get_de_by_id(de_id)
    if de_resp["status"] == 1:
        return de_resp

    # If in no_trust mode, we need to record this REMOVE_DATA to wal
    if write_ahead_log:
        wal_entry = f"database_api.remove_de_by_id({de_id})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    return database_api.remove_de_by_id(de_id)

def list_all_des_with_src():
    get_all_des_res = database_api.get_all_des()
    if get_all_des_res["status"] == 1:
        return get_all_des_res
    res = []
    for de in get_all_des_res["data"]:
        de_id = de.id
        # print(de_id)
        src_agent_id_resp = database_api.get_de_owner_id(de_id)
        if src_agent_id_resp["status"] == 1:
            return src_agent_id_resp
        res.append({"DE": de_id, "Source Agent": src_agent_id_resp["data"]})
    return res
