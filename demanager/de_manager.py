import pathlib

from dbservice import database_api
from common import common_procedure


def register_de_in_DB(de_id,
                      de_name,
                      user_id,
                      de_type,
                      access_param,
                      optimistic,
                      write_ahead_log,
                      key_manager, ):
    # Call DB to register a new data element in the database

    if pathlib.Path(access_param).is_file():
        access_param = str(pathlib.Path(access_param).absolute())

    # If in no_trust mode, we need to record this ADD_DATA to wal
    if write_ahead_log:
        wal_entry = f"database_api.create_de({de_id}, {de_name}, {user_id}, {de_type}, {access_param}, {optimistic})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    de_resp = database_api.create_de(int(de_id),
                                     str(de_name),
                                     int(user_id),
                                     str(de_type),
                                     str(access_param),
                                     bool(optimistic), )
    if de_resp["status"] == 1:
        return de_resp

    return {"status": de_resp["status"], "message": de_resp["message"], "de_id": de_resp["data"].id}


def remove_de(de_name,
              cur_username,
              write_ahead_log,
              key_manager, ):
    # Check if there is an existing data element
    de_resp = database_api.get_de_by_name(de_name)
    if de_resp["status"] == 1:
        return de_resp

    # print(de_resp.data[0])

    # Get ID of the DE
    de_id = de_resp["data"].id
    type_of_de = de_resp["data"].type

    # Check if there is an existing user
    user_resp = database_api.get_user_by_user_name(cur_username)
    if user_resp["status"] == 1:
        return user_resp
    cur_user_id = user_resp["data"].id

    # Check if the DE owner is the current user
    verify_owner_response = common_procedure.verify_de_owner(de_id, cur_username)
    if verify_owner_response.status == 1:
        return verify_owner_response

    # Actually remove the DE
    # If in no_trust mode, we need to record this REMOVE_DATA to wal
    if write_ahead_log is not None:
        wal_entry = f"database_api.remove_de_by_name({de_name})"
        # If write_ahead_log is not None, key_manager also will not be None
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    database_service_response = database_api.remove_de_by_name(de_name)
    if database_service_response["status"] == 1:
        return database_service_response

    return {"status": 0,
            "message": "Successfully removed data",
            "de_id": de_id,
            "type": type_of_de, }


def list_discoverable_des():
    database_service_response = database_api.list_discoverable_des()
    res = []
    for discoverable_de_id in database_service_response:
        res.append(discoverable_de_id[0])
    return res
