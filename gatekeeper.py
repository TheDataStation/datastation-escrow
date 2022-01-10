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

def brokerAccess(user_id, api, exe_mode):
    policy_info = policyWithDependency.get_user_api_info(user_id, api)
    accessible_set = policy_info.accessible_data
    need_to_access = []
    listOfFiles = list()
    # First case: we are running the optimistic execution mode
    if exe_mode == "optimistic":
        if policy_info.odata_type == "dir_accessible":
            # First fill in need_to_access
            # In optimistic mode, need_to_access == all data in DB
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
            elif api == "ModelTrain":
                api_res = ModelTrain("Working")

            # Generate ID for api_res
            res_id = random.randint(100001, 200000)

            # Get status for api_res
            status = set(need_to_access).issubset(set(accessible_set))

            # Now that we have finished executing the function, we remove the working directory
            shutil.rmtree("Working")
    # Second case: we are running the optimistic execution mode
    else:
        if policy_info.odata_type == "dir_accessible":
            # First fill in need_to_access
            # In pessimistic mode, need_to_access == accessible_set
            need_to_access = accessible_set
            # Then fill in the path of all available files (in string format)
            SM_storage_path = "SM_storage"
            for (dirpath, dirnames, filenames) in os.walk(SM_storage_path):
                listOfFiles += [os.path.join(dirpath, file) for file in filenames]
            # In pessimistic mode with dir_accessible,
            # we filter out files that cannot be accessed
            listOfFiles = list(filter(lambda x: int(x.split("/")[1]) in accessible_set, listOfFiles))
            # Create a working directory
            os.mkdir("Working")
            # Copy the files over
            for f in listOfFiles:
                shutil.copy(f, "Working")

            # In pessimistic mode, we only execute the function if policy check passes
            if set(need_to_access).issubset(set(accessible_set)):
                # Get function output
                if api == "Preprocess":
                    api_res = DataPreprocess("Working")
                elif api == "ModelTrain":
                    api_res = ModelTrain("Working")

                # Generate ID for api_res
                res_id = random.randint(100001, 200000)

                # Get status for api_res
                status = True

                # Now that we have finished executing the function, we remove the working directory
                shutil.rmtree("Working")
            else:
                api_res = None
                res_id = -1
                status = False

    # Lastly, prepare the dictionary to be returned
    dict_res = dict()
    dict_res["data"] = api_res
    dict_res["outputID"] = res_id
    dict_res["status"] = status
    dict_res["accessed_data"] = need_to_access
    dict_res["accessible_data"] = accessible_set

    return dict_res
