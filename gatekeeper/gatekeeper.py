from dsapplicationregistration.dsar_core import (register_connectors,
                                                 get_names_registered_functions,
                                                 get_registered_functions,
                                                 get_registered_dependencies,)
from dbservice import database_api
from policybroker import policy_broker
from models.api import *
from models.api_dependency import *
from models.user import *
from models.response import *

def gatekeeper_setup(connector_name, connector_module_path):
    # print("Start setting up the gatekeeper")
    register_connectors(connector_name, connector_module_path)
    # print("Check registration results:")
    # TODO: change this part once the interceptor has been added in
    # add dependency X -> "dir_accessible" if X does not depend on anything else
    apis_to_register = get_names_registered_functions()
    dependencies_to_register = get_registered_dependencies()
    for cur_api in apis_to_register:
        if cur_api not in dependencies_to_register.keys():
            dependencies_to_register[cur_api] = ["dir_accessible"]
    # add "dir_accessible" to list of apis
    if "dir_accessible" not in apis_to_register:
        apis_to_register.append("dir_accessible")
    # print(apis_to_register)
    # print(dependencies_to_register)

    # now we call dbservice to register these info in the DB
    for cur_api in apis_to_register:
        api_db = API(api_name=cur_api)
        database_service_response = database_api.create_api(api_db)
        if database_service_response.status == -1:
            return Response(status=1, message="internal database error")
    for cur_from_api in dependencies_to_register:
        to_api_list = dependencies_to_register[cur_from_api]
        for cur_to_api in to_api_list:
            api_dependency_db = APIDependency(from_api=cur_from_api,
                                              to_api=cur_to_api,)
            database_service_response = database_api.create_api_dependency(api_dependency_db)
            if database_service_response.status == -1:
                return Response(status=1, message="internal database error")
    return Response(status=0, message="Gatekeeper setup success")


def get_accessible_data(user_id, api):
    policy_info = policy_broker.get_user_api_info(user_id, api)
    return policy_info


def call_api(api, cur_username, data_station_log, *args, **kwargs):

    # TODO: fill the actual log operations in
    data_station_log._log(api)

    # TODO: add the intent-policy matching process in here

    # get current user id
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username,))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    # look at the accessible data by policy for current (user, api)
    policy_info = get_accessible_data(cur_user_id, api)
    accessible_data_policy = policy_info.accessible_data

    # look at all optimistic data from the DB
    optimistic_data= database_api.get_all_optimistic_datasets()
    accessible_data_optimistic = []
    for i in range(len(optimistic_data.data)):
        cur_optimistic_id = optimistic_data.data[i].id
        accessible_data_optimistic.append(cur_optimistic_id)

    # Combine these two types of accessible data elements together
    all_accessible_data_id = set(accessible_data_policy + accessible_data_optimistic)
    print("all accessible data elements are: ")
    print(all_accessible_data_id)

    # zz: create a working dir from all_accessible_data_id
    # zz: mount the working dir to mount point that encodes user_id and api name using interceptor
    # zz: run api
    # zz: record all data ids that are accessed by the api through interceptor
    # zz: check whether access to those data ids is valid, if not we cannot release the results

    # Getting these data elements from the DB
    all_accessible_data = filter(lambda data: data.id in all_accessible_data_id,
                                 database_api.get_all_datasets().data)
    # print("looking at the access types:")
    # for cur_data in all_accessible_data:
    #     print(cur_data.access_type)

    # Acutally calling the api
    list_of_apis = get_registered_functions()
    for cur_api in list_of_apis:
        if api == cur_api.__name__:
            return cur_api(*args, **kwargs)
    return "Error"
