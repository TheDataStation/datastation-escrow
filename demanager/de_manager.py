import pathlib

from dbservice import database_api
from common import common_procedure
from common.pydantic_models.dataset import Dataset
from common.pydantic_models.user import User


def register_data_in_DB(de_id,
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
        wal_entry = "database_api.create_dataset(Dataset(id=" + str(de_id) \
                    + ",name='" + data_name \
                    + "',owner_id=" + str(user_id) \
                    + ",type='" + data_type \
                    + "',access_param='" + access_param \
                    + "',optimistic=" + str(optimistic) \
                    + "))"
        write_ahead_log.log(user_id, wal_entry, key_manager, )

    new_dataset = Dataset(id=de_id,
                          name=data_name,
                          owner_id=user_id,
                          type=data_type,
                          access_param=access_param,
                          optimistic=optimistic)

    database_service_response = database_api.create_dataset(new_dataset)
    if database_service_response.status == -1:
        return {"status": 1, "message": "internal database error"}

    return {"status": 0, "message": "success", "de_id": de_id}

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
            "de_id": dataset_id,
            "type": type_of_data, }

def list_discoverable_des():
    database_service_response = database_api.list_discoverable_des()
    res = []
    for discoverable_de_id in database_service_response:
        res.append(discoverable_de_id[0])
    return res
