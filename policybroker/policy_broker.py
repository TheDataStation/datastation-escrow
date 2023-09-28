from dbservice import database_api
from common import common_procedure
import os


# Helper function to get all ancestors of an api from dependency graph (including itself)
def get_all_ancestors(api, d_graph, cur_ancestors):
    if d_graph[api]:
        cur_ancestors += d_graph[api]
        for parent_api in d_graph[api]:
            get_all_ancestors(parent_api, d_graph, cur_ancestors)

# get all policies from DB

def get_all_apis():
    # get list of APIs from DB
    api_res = database_api.get_all_apis()
    list_of_apis = list(api_res.data)
    return list_of_apis


def get_all_dependencies():
    list_of_dependencies = []
    # get list of API dependencies from DB, and fill in dependency_graph
    api_depend_res = database_api.get_all_api_dependencies()
    for i in range(len(api_depend_res.data)):
        from_api = api_depend_res.data[i].from_api
        to_api = api_depend_res.data[i].to_api
        cur_tuple = (from_api, to_api)
        # Fill in list of dependencies
        list_of_dependencies.append(cur_tuple)
    return list_of_dependencies


# For gatekeeper

def get_user_api_info(user_id, api, share_id):
    list_of_policies = []
    list_of_dependencies = []
    dependency_graph = dict()

    # get list of APIs from DB
    api_res = database_api.get_all_apis()
    list_of_apis = list(api_res.data)

    # Initialize dependency_graph using the list of apis as keys
    for cur_api in list_of_apis:
        dependency_graph[cur_api] = []

    # get list of API dependencies from DB, and fill in dependency_graph
    api_depend_res = database_api.get_all_api_dependencies()
    for i in range(len(api_depend_res.data)):
        from_api = api_depend_res.data[i].from_api
        to_api = api_depend_res.data[i].to_api
        cur_tuple = (from_api, to_api)
        # Fill in list of dependencies
        list_of_dependencies.append(cur_tuple)
        # Fill in dependency_graph dict
        dependency_graph[to_api].append(from_api)

    # get ancestors of the api being called
    cur_ancestors = [api]
    print("get_user_api_info, with pid: ", os.getpid(), api, dependency_graph, cur_ancestors)
    get_all_ancestors(api, dependency_graph, cur_ancestors)
    # print("Current api's ancestors are:")
    # print(cur_ancestors)

    # get list of policies for the current user
    policy_res = database_api.get_ack_policy_user_share(user_id, share_id)
    for i in range(len(policy_res.data)):
        cur_tuple = (policy_res.data[i].user_id,
                     policy_res.data[i].api,
                     policy_res.data[i].data_id,)
        list_of_policies.append(cur_tuple)

    # get all accessible data for current <user_id, api>
    accessible_data = []
    for policy in list_of_policies:
        if policy[1] in cur_ancestors:
            accessible_data.append(policy[2])
    accessible_data = list(set(accessible_data))

    # # Check correctness
    # print("accessible data:", accessible_data)

    return accessible_data
