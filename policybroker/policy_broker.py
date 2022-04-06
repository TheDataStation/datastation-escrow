# from common.policy_info import *
from dbservice import database_api
from common import common_procedure
from common.pydantic_models.policy import Policy
from common.pydantic_models.user import User
from common.pydantic_models.response import Response


# Helper function to get the odata_type used in policy_with_dependency
def get_odata_type(api, d_graph):
    first_child_api = d_graph[api][0]
    return get_odata_type(first_child_api, d_graph)

# Helper function to get all ancestors of an api from dependency graph (including itself)
def get_all_ancestors(api, d_graph, cur_ancestors):
    if d_graph[api]:
        cur_ancestors += d_graph[api]
        for parent_api in d_graph[api]:
            get_all_ancestors(parent_api, d_graph, cur_ancestors)

# upload a new policy to DB

def upload_policy(policy: Policy,
                  cur_username,
                  write_ahead_log=None,
                  key_manager=None,
                  check_point=None,):
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
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, check_point, )

    response = database_api.create_policy(policy)
    return Response(status=response.status, message=response.msg)

# upload policies in bulk fashion

def bulk_upload_policies(policies,
                         cur_username,
                         write_ahead_log=None,
                         key_manager=None,
                         check_point=None, ):
    # First check if the dataset owner(s) is the current user
    for policy in policies:
        verify_owner_response = common_procedure.verify_dataset_owner(policy.data_id, cur_username)
        if verify_owner_response.status == 1:
            return verify_owner_response
    # print("all owner check passed")

    # Get caller's UID
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id
    # print(cur_user_id)

    # If in no_trust mode, we need to record EVERY add_policy to wal
    if write_ahead_log is not None:
        for policy in policies:
            wal_entry = "database_api.create_policy(Policy(user_id=" + str(policy.user_id) \
                        + ",api='" + policy.api \
                        + "',data_id=" + str(policy.data_id) \
                        + "))"
            # If write_ahead_log is not None, key_manager also will not be None
            write_ahead_log.log(cur_user_id, wal_entry, key_manager, check_point, )

    response = database_api.bulk_upload_policies(policies)
    return response

# remove a policy from DB

def remove_policy(policy: Policy,
                  cur_username,
                  write_ahead_log=None,
                  key_manager=None,
                  check_point=None,):
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
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, check_point, )

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

def get_user_api_info(user_id, api):

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
    get_all_ancestors(api, dependency_graph, cur_ancestors)
    # print("Current api's ancestors are:")
    # print(cur_ancestors)

    # get list of policies for the current user
    policy_res = database_api.get_policy_for_user(user_id)
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
    # print("accessible data from new method:")
    # print(accessible_data)

    return accessible_data
