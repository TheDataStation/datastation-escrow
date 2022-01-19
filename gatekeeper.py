import os
import shutil

from models.derived import *
from models.provenance import *

from dbservice import database_api

import random

import policy_broker
from titanicML.titanic import data_preprocess, model_train, predict

def broker_access(user_id, api, exe_mode, data=None):
    policy_info = policy_broker.get_user_api_info(user_id, api)
    policy_broker.get_user_api_info_two(user_id, api)
    print(policy_info.accessible_data)
    print(policy_info.odata_type)
    accessible_set = policy_info.accessible_data
    need_to_access = []
    listOfFiles = list()
    # First case: we are running the optimistic execution mode
    if exe_mode == "optimistic":
        if policy_info.odata_type == "dir_accessible":
            # First fill in need_to_access
            # In optimistic mode, need_to_access == all data in DB
            all_datasets = database_api.get_all_datasets()
            for i in range(len(all_datasets.data)):
                cur_id = all_datasets.data[i].id
                need_to_access.append(cur_id)
            # print(need_to_access)
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
                api_res = data_preprocess("Working")
            elif api == "ModelTrain":
                api_res = model_train("Working")
            elif api == "Predict":
                # Create another temporary csv file to hold the params
                f = open("cur_api_input.csv", 'wb')
                f.write(data)
                f.close()
                api_res = predict("Working", "cur_api_input.csv")
                os.remove("cur_api_input.csv")

            # Generate ID for api_res
            res_id = random.randint(100001, 200000)

            # Get status for api_res
            status = set(need_to_access).issubset(set(accessible_set))

            # Now that we have finished executing the function, we remove the working directory
            shutil.rmtree("Working")
    # Second case: we are running the pessimistic execution mode
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
                    api_res = data_preprocess("Working")
                elif api == "ModelTrain":
                    api_res = model_train("Working")
                elif api == "Predict":
                    # Create another temporary csv file to hold the params
                    f = open("cur_api_input.csv", 'wb')
                    f.write(data)
                    f.close()
                    api_res = predict("Working", "cur_api_input.csv")
                    # print(api_res)
                    os.remove("cur_api_input.csv")

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

    # Before we return, we have enough information to record this derived data in Derived DB
    derived_response = database_api.create_derived(Derived(id=res_id,
                                                           caller_id=user_id,
                                                           api=api,))
    if derived_response.status == -1:
        return {"status": -1}

    # Then we capture the provenance information using a loop
    for data_id in need_to_access:
        # print(data_id)
        cur_prov_resp = database_api.create_provenance(Provenance(child_id=res_id,
                                                                  parent_id=data_id,))
        if cur_prov_resp.status == -1:
            return {"status": -1}

    # Lastly, prepare the dictionary to be returned
    dict_res = dict()
    dict_res["data"] = api_res
    dict_res["outputID"] = res_id
    dict_res["status"] = status
    dict_res["accessed_data"] = need_to_access
    dict_res["accessible_data"] = accessible_set

    return dict_res
