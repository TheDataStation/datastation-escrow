import grpc
import database_pb2
import database_pb2_grpc
from models.response import *
import random
import storage_manager

database_service_channel = grpc.insecure_channel('localhost:50051')
database_service_stub = database_pb2_grpc.DatabaseStub(database_service_channel)

def upload_data(data_name, data):

    # check if there is an existing dataset

    existed_dataset = database_service_stub.GetDatasetByName(database_pb2.Dataset(name=data_name))
    if existed_dataset.status == 1:
        return Response(status=1, message="there is a dataset using the same name")

    # call identity_manager to compute the dataset id

    dataset_id = random.randint(1, 100000)

    # We now call DB to register a new dataset in the database

    new_dataset = database_pb2.Dataset(id=dataset_id,
                                       name=data_name,)
    database_service_response = database_service_stub.CreateDataset(new_dataset)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    # Registration of the dataset is successful. Now we call SM
    storage_manager_response = storage_manager.Store(data_name, dataset_id, data)
    # If dataset already exists
    if storage_manager_response.status == 1:
        return storage_manager_response

    # Successful
    return UploadDataResponse(status=0, message="success", data_id=dataset_id)

def remove_data(data_name):

    # Step 1: check if there is an existing dataset
    existed_dataset = database_service_stub.GetDatasetByName(database_pb2.Dataset(name=data_name))

    if existed_dataset.status == -1:
        return Response(status=1, message="Dataset does not exist.")

    # Step 2: if exists, remove it
    dataset_to_remove = database_pb2.Dataset(name=data_name,)
    database_service_response = database_service_stub.RemoveDatasetByName(dataset_to_remove)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    # Step 3: at this step we have removed the record about the dataset from DB
    # Now we remove its actual content from SM

    # Get ID of the dataset
    dataset_id = existed_dataset.data[0].id

    storage_manager_response = storage_manager.Remove(data_name, dataset_id)

    # If SM removal failed
    if storage_manager_response.status == 1:
        return storage_manager_response

    return Response(status=0, message="Successfully removed data")
