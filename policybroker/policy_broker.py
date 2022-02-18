from common.policy_info import *
from dbservice import database_api
from common import common_procedure
from models.policy import *
from models.user import *
from models.response import *


# Helper function to get the odata_type used in policy_with_dependency
def get_odata_type(api, d_graph):
    if "dir_accessible" in d_graph[api]:
        return "dir_accessible"
    else:
        first_child_api = d_graph[api][0]
        return get_odata_type(first_child_api, d_graph)

# Helper function to update a policy's effect in policy_with_dependency
def update_policy_effect(api, data_id, d_graph, policy_dict):
    cur_key = api
    cur_policy_info = policy_dict[cur_key]
    # If the current API can access the current data_id, we don't have to do anything
    # Because we know all its descendants will be able to access this data_id as well
    # So we only do something if the current API cannot access the current data_id
    if data_id not in cur_policy_info.accessible_data:
        policy_dict[cur_key].accessible_data.append(data_id)
        if not d_graph[cur_key] == ["dir_accessible"]:
            for child_api in d_graph[cur_key]:
                update_policy_effect(child_api, data_id, d_graph, policy_dict)

# upload a new policy to DB

def upload_policy(policy: Policy,
                  cur_username,
                  write_ahead_log=None,
                  key_manager=None,):
    # First check if the dataset owner is the current user
    verify_owner_response = common_procedure.verify_dataset_owner(policy.data_id, cur_username)
    if verify_owner_response.status == 1:
        return verify_owner_response

    # Get caller's id
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    # If in no_trust mode, we need to record this ADD_POLICY to wal
    if write_ahead_log is not None:
        wal_entry = "database_api.create_policy(Policy(user_id=" + str(policy.user_id) \
                    + ",api='" + policy.api \
                    + "',data_id=" + str(policy.data_id) \
                    + "))"
        # If write_ahead_log is not None, key_manager also will not be None
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    response = database_api.create_policy(policy)
    return Response(status=response.status, message=response.msg)

# remove a policy from DB

def remove_policy(policy: Policy,
                  cur_username,
                  write_ahead_log=None,
                  key_manager=None,):
    # First check if the dataset owner is the current user
    verify_owner_response = common_procedure.verify_dataset_owner(policy.data_id, cur_username)
    if verify_owner_response.status == 1:
        return verify_owner_response

    # Get caller's id
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    # If in no_trust mode, we need to record this ADD_POLICY to wal
    if write_ahead_log is not None:
        wal_entry = "database_api.remove_policy(Policy(user_id=" + str(policy.user_id) \
                    + ",api='" + policy.api \
                    + "',data_id=" + str(policy.data_id) \
                    + "))"
        # If write_ahead_log is not None, key_manager also will not be None
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    response = database_api.remove_policy(policy)
    return Response(status=response.status, message=response.msg)

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

def get_all_policies():
    list_of_policies = []
    # get list of policies for the current user
    policy_res = database_api.get_all_policies()
    for i in range(len(policy_res.data)):
        cur_tuple = (policy_res.data[i].user_id,
                     policy_res.data[i].api,
                     policy_res.data[i].data_id,)
        list_of_policies.append(cur_tuple)
    return list_of_policies

# For gatekeeper
# TODO: the efficiency of this function can be improved
# TODO: Right now we are adding the effect of all policies for a user, but we only need all ancestors of current API.
# TODO: Improve this if efficiency becomes a problem.
def get_user_api_info(user_id, api):

    # Initialize the variables needed
    list_of_policies = []
    list_of_dependencies = []
    dependency_graph = dict()
    policy_with_dependency = dict()

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
        dependency_graph[from_api].append(to_api)

    # get list of policies for the current user
    policy_res = database_api.get_policy_for_user(user_id)
    for i in range(len(policy_res.data)):
        cur_tuple = (policy_res.data[i].user_id,
                     policy_res.data[i].api,
                     policy_res.data[i].data_id,)
        list_of_policies.append(cur_tuple)

    # Initialize cur_policy_info from dependency_graph
    for key in dependency_graph:
        cur_accessible_data = []
        cur_odata_type = get_odata_type(key, dependency_graph)
        cur_policy_info = PolicyInfo(cur_accessible_data, cur_odata_type)
        policy_with_dependency[key] = cur_policy_info

    # From list_of_policies, fill in policy_with_dependency with policies
    for policy in list_of_policies:
        update_policy_effect(policy[1], policy[2], dependency_graph, policy_with_dependency)

    # Check if policy_with_dependency is correct
    # for key, value in policy_with_dependency.items():
    #     print(key)
    #     print(value.accessible_data)
    #     print(value.odata_type)

    return policy_with_dependency[api]
