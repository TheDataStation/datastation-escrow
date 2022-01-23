from models.response import *
import random
import storage_manager
from dbservice import database_api
from models.dataset import *
from models.user import *

def upload_data(data_name, data, cur_username):

    # check if there is an existing dataset

    existed_dataset = database_api.get_dataset_by_name(Dataset(name=data_name,))
    if existed_dataset.status == 1:
        return Response(status=1, message="there is a dataset using the same name")

    # call identity_manager to compute the dataset id

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

    # Registration of the dataset is successful. Now we call SM
    storage_manager_response = storage_manager.Store(data_name, dataset_id, data)
    # If dataset already exists
    if storage_manager_response.status == 1:
        return storage_manager_response

    # Successful
    return UploadDataResponse(status=0, message="success", data_id=dataset_id)

def remove_data(data_name, cur_username):

    # Step 1: check if there is an existing dataset
    existed_dataset = database_api.get_dataset_by_name(Dataset(name=data_name,))

    if existed_dataset.status == -1:
        return Response(status=1, message="Dataset does not exist.")

    # Get ID of the dataset
    dataset_id = existed_dataset.data[0].id

    # Step 2: if exists, check if the dataset owner is the current user

    # get dataset owner id
    dataset_owner = database_api.get_dataset_owner(Dataset(id=dataset_id,))
    if dataset_owner.status == -1:
        return Response(status=1, message="Error retrieving data owner.")
    dataset_owner_id = dataset_owner.data[0].id
    # print("Dataset owner id is: " + str(dataset_owner_id))

    # get current user id
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id
    # print("Current user id is: "+str(cur_user_id))

    if cur_user_id != dataset_owner_id:
        return Response(status=1, message="Current user is not owner of dataset")

    # Step 3: actually remove the dataset
    dataset_to_remove = Dataset(name=data_name,)
    database_service_response = database_api.remove_dataset_by_name(dataset_to_remove)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    # Step 3: at this step we have removed the record about the dataset from DB
    # Now we remove its actual content from SM

    storage_manager_response = storage_manager.Remove(data_name, dataset_id)

    # If SM removal failed
    if storage_manager_response.status == 1:
        return storage_manager_response

    return Response(status=0, message="Successfully removed data")
