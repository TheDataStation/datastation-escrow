import grpc
import database_pb2
import database_pb2_grpc

import os
import shutil

import random

import policyWithDependency
from titanicML.titanic import DataPreprocess, ModelTrain, Predict

database_service_channel = grpc.insecure_channel('localhost:50051')
database_service_stub = database_pb2_grpc.DatabaseStub(database_service_channel)

def brokerAccess_opt(user_id, api):
    policy_info = policyWithDependency.get_user_api_info(user_id, api)
    accessible_set = policy_info.accessible_data
    need_to_access = []
    listOfFiles = list()
    if policy_info.odata_type == "dir_accessible":
        # First fill in need_to_access
        all_datasets = database_service_stub.GetAllDatasets(database_pb2.DBEmpty())
        for i in range(len(all_datasets.data)):
            cur_id = all_datasets.data[i].id
            need_to_access.append(cur_id)
        # Then fill in the path of all available files (in string format)
        SM_storage_path = "SM_storage"
        for (dirpath, dirnames, filenames) in os.walk(SM_storage_path):
            listOfFiles += [os.path.join(dirpath, file) for file in filenames]
        # Create a working directory
        os.mkdir("Working")
        # Copy the files over
        for f in listOfFiles:
            shutil.copy(f, "Working")

        # Now we have all the needed files for function execution in one place
        # We can execute the actual function

        # Get function output
        if api == "Preprocess":
            api_res = DataPreprocess("Working")

        # Generate ID for api_res
        res_id = random.randint(100001, 200000)

        # Get status for api_res
        status = set(need_to_access).issubset(set(accessible_set))
        print(status)

        # Now that we have finished executing the function, we remove the working directory
        shutil.rmtree("Working")

    return policy_info
