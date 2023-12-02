import pathlib

from dbservice import database_api
from common import common_procedure


def register_de_in_DB(de_id,
                      de_name,
                      user_id,
                      de_type,
                      access_param,
                      discoverable,
                      write_ahead_log,
                      key_manager, ):
    # Call DB to register a new data element in the database

    if pathlib.Path(access_param).is_file():
        access_param = str(pathlib.Path(access_param).absolute())

    # If in no_trust mode, we need to record this ADD_DATA to wal
    if write_ahead_log:
        wal_entry = f"database_api.create_de({de_id}, {de_name}, {user_id}, {de_type}, {access_param}, {discoverable})"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    de_resp = database_api.create_de(int(de_id),
                                     str(de_name),
                                     int(user_id),
                                     str(de_type),
                                     str(access_param),
                                     bool(discoverable), )
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

def list_discoverable_des():
    database_service_response = database_api.list_discoverable_des()
    res = []
    for discoverable_de_id in database_service_response:
        res.append(discoverable_de_id[0])
    return res
