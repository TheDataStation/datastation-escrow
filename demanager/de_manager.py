import pathlib

from dbservice import database_api
from common import common_procedure
from common.pydantic_models.dataset import Dataset
from common.pydantic_models.staged import Staged
from common.pydantic_models.user import User


def register_data_in_DB(data_id,
                        data_name,
                        user_id,
                        data_type,
                        access_param,
                        optimistic,
                        write_ahead_log=None,
                        key_manager=None):

    # Call DB to register a new dataset in the database

    if pathlib.Path(access_param).is_file():
        access_param = str(pathlib.Path(access_param).absolute())

    # If in no_trust mode, we need to record this ADD_DATA to wal
    if write_ahead_log is not None:
        wal_entry = "database_api.create_dataset(Dataset(id=" + str(data_id) \
                    + ",name='" + data_name \
                    + "',owner_id=" + str(user_id) \
                    + ",type='" + data_type \
                    + "',access_param='" + access_param \
                    + "',optimistic=" + str(optimistic) \
                    + "))"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    new_dataset = Dataset(id=data_id,
                          name=data_name,
                          owner_id=user_id,
                          type=data_type,
                          access_param=access_param,
                          optimistic=optimistic)

    database_service_response = database_api.create_dataset(new_dataset)
    if database_service_response.status == -1:
        return {"status": 1, "message": "internal database error"}

    return {"status": 0, "message": "success", "de_id": data_id}

def remove_data(data_name,
                cur_username,
                write_ahead_log=None,
                key_manager=None,):

    # Check if there is an existing data element
    existed_dataset = database_api.get_dataset_by_name(Dataset(name=data_name,))
    if existed_dataset.status == -1:
        return {"status": 1, "message": "DE does not exist."}

    # print(existed_dataset.data[0])

    # Get ID of the dataset
    dataset_id = existed_dataset.data[0].id
    type_of_data = existed_dataset.data[0].type

    # Check if there is an existing user
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    if cur_user.status == -1:
        return {"status": 1, "message": "Something wrong with the current user"}
    cur_user_id = cur_user.data[0].id

    # Check if the dataset owner is the current user
    verify_owner_response = common_procedure.verify_dataset_owner(dataset_id, cur_username)
    if verify_owner_response.status == 1:
        return verify_owner_response

    # Actually remove the dataset
    # If in no_trust mode, we need to record this REMOVE_DATA to wal
    if write_ahead_log is not None:
        wal_entry = "database_api.remove_dataset_by_name(Dataset(name='" + data_name \
                    + "'))"
        # If write_ahead_log is not None, key_manager also will not be None
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    dataset_to_remove = Dataset(name=data_name,)
    database_service_response = database_api.remove_dataset_by_name(dataset_to_remove)
    if database_service_response.status == -1:
        return {"status": 1, "message": "database error: remove DE failed"}

    return {"status": 0,
            "message": "Successfully removed data",
            "data_id": dataset_id,
            "type": type_of_data, }

def register_staged_in_DB(data_id,
                          caller_id,
                          api,
                          write_ahead_log=None,
                          key_manager=None,):
    wal_entry = "database_api.create_staged(Staged(id=" + str(data_id) \
                + ",caller_id=" + str(caller_id) \
                + ",api='" + api \
                + "'))"
    # If in no_trust mode, record this entry
    if write_ahead_log is not None:
        write_ahead_log.log(caller_id, wal_entry, key_manager, )

    # Start writing the entry to DB
    new_staged = Staged(id=data_id,
                        caller_id=caller_id,
                        api=api,)
    database_service_response = database_api.create_staged(new_staged)
    if database_service_response.status == -1:
        return {"status": 1, "message": "database error: create staged DE failed"}

    return {"status": 0, "message": "success"}

def register_provenance_in_DB(data_id,
                              data_ids_accessed,
                              cur_user_id=None,
                              write_ahead_log=None,
                              key_manager=None,):
    wal_entry = "database_api.bulk_create_provenance(" + str(data_id) \
                + ", " + str(list(data_ids_accessed)) \
                + ")"
    # If in no_trust mode, record this entry
    if write_ahead_log is not None:
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    database_service_response = database_api.bulk_create_provenance(data_id, list(data_ids_accessed))
    if database_service_response == 1:
        return {"status": 1, "message": "internal database error"}

    return {"status": 0, "message": "success"}

def list_discoverable_des():
    database_service_response = database_api.list_discoverable_des()
    res = []
    for discoverable_de_id in database_service_response:
        res.append(discoverable_de_id[0])
    return res
