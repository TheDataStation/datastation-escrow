import pathlib

from dbservice import database_api
from common import common_procedure
from common.pydantic_models.dataset import Dataset
from common.pydantic_models.user import User
from common.pydantic_models.response import Response, UploadDataResponse, RemoveDataResponse


def register_data_in_DB(data_id,
                        data_name,
                        cur_username,
                        data_type,
                        access_type,
                        optimistic,
                        write_ahead_log=None,
                        key_manager=None,
                        original_data_size=None):

    # # TODO: check if there is an existing dataset
    #
    # existed_dataset = database_api.get_dataset_by_name(Dataset(name=data_name,))
    # if existed_dataset.status == 1:
    #     return Response(status=1, message="there is a dataset using the same name")

    # We now call DB to register a new dataset in the database

    # check if there is an existing user
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username,))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    if pathlib.Path(access_type).is_file():
        access_type = str(pathlib.Path(access_type).absolute())

    # If in no_trust mode, we need to record this ADD_DATA to wal
    if write_ahead_log is not None:
        wal_entry = "database_api.create_dataset(Dataset(id=" + str(data_id) \
                    + ",name='" + data_name \
                    + "',owner_id=" + str(cur_user_id) \
                    + ",type='" + data_type \
                    + "',access_type='" + access_type \
                    + "',optimistic=" + str(optimistic) \
                    + "))"
        # If write_ahead_log is not None, key_manager also will not be None
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    new_dataset = Dataset(id=data_id,
                          name=data_name,
                          owner_id=cur_user_id,
                          type=data_type,
                          access_type=access_type,
                          optimistic=optimistic,
                          original_data_size=original_data_size)
    database_service_response = database_api.create_dataset(new_dataset)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    return UploadDataResponse(status=0, message="success", data_id=data_id)

def remove_data(data_name,
                cur_username,
                write_ahead_log=None,
                key_manager=None,):

    # Step 1: check if there is an existing dataset
    existed_dataset = database_api.get_dataset_by_name(Dataset(name=data_name,))

    if existed_dataset.status == -1:
        return Response(status=1, message="Dataset does not exist.")

    # print(existed_dataset.data[0])

    # Get ID of the dataset
    dataset_id = existed_dataset.data[0].id
    type_of_data = existed_dataset.data[0].type

    # check if there is an existing user
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    # Step 2: if exists, check if the dataset owner is the current user
    verify_owner_response = common_procedure.verify_dataset_owner(dataset_id, cur_username)
    if verify_owner_response.status == 1:
        return verify_owner_response

    # Step 3: actually remove the dataset
    # If in no_trust mode, we need to record this REMOVE_DATA to wal
    if write_ahead_log is not None:
        wal_entry = "database_api.remove_dataset_by_name(Dataset(name='" + data_name \
                    + "'))"
        # If write_ahead_log is not None, key_manager also will not be None
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    dataset_to_remove = Dataset(name=data_name,)
    database_service_response = database_api.remove_dataset_by_name(dataset_to_remove)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    return RemoveDataResponse(status=0,
                              message="Successfully removed data",
                              data_id=dataset_id,
                              type=type_of_data,)
