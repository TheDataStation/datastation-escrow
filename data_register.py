import random
import storage_manager
from dbservice import database_api
from common import common_procedure
from models.dataset import *
from models.user import *
from models.response import *


def upload_data(data_name, cur_username):

    # TODO: check if there is an existing dataset

    existed_dataset = database_api.get_dataset_by_name(Dataset(name=data_name,))
    if existed_dataset.status == 1:
        return Response(status=1, message="there is a dataset using the same name")

    # TODO: call identity_manager to compute the dataset id

    dataset_id = random.randint(1, 100000)

    # We now call DB to register a new dataset in the database

    # check if there is an existing user
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username,))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    new_dataset = Dataset(id=dataset_id,
                          name=data_name,
                          owner_id=cur_user_id,)
    database_service_response = database_api.create_dataset(new_dataset)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    return UploadDataResponse(status=0, message="success", data_id=dataset_id)

def remove_data(data_name, cur_username):

    # Step 1: check if there is an existing dataset
    existed_dataset = database_api.get_dataset_by_name(Dataset(name=data_name,))

    if existed_dataset.status == -1:
        return Response(status=1, message="Dataset does not exist.")

    # Get ID of the dataset
    dataset_id = existed_dataset.data[0].id

    # Step 2: if exists, check if the dataset owner is the current user
    verify_owner_response = common_procedure.verify_dataset_owner(dataset_id, cur_username)
    if verify_owner_response.status == 1:
        return verify_owner_response

    # Step 3: actually remove the dataset
    dataset_to_remove = Dataset(name=data_name,)
    database_service_response = database_api.remove_dataset_by_name(dataset_to_remove)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    return RemoveDataResponse(status=0, message="Successfully removed data", data_id=dataset_id)
