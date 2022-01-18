from classes.policy_info import *
from dbservice import database_api

list_of_apis = []
list_of_dependencies = []
list_of_policies = []
list_of_uid = []

# Initialize dependency graph as a dict
dependency_graph = dict()

# Initialize PolicyWithDependency as a dict
# policy_with_dependency has the following data structure:
# key: (int, str) tuple
# value: Class with 2 attrs:
#           accessible: list[int]
#           odata_access_type: str
policy_with_dependency = dict()

# Helper function to get the odata_type used in policy_with_dependency
def get_odata_type(api, d_graph):
    if "dir_accessible" in d_graph[api]:
        return "dir_accessible"
    else:
        first_child_api = d_graph[api][0]
        return get_odata_type(first_child_api, d_graph)

# Helper function to update a policy's effect in policy_with_dependency
def update_policy_effect(user_id, api, data_id):
    # Base case: current API does not point to other APIs
    # Right now it means dependency_graph[api] == ["dir_accessible"]
    if dependency_graph[api] == ["dir_accessible"]:
        cur_key = (user_id, api)
        cur_policy_info = policy_with_dependency[cur_key]
        if data_id not in cur_policy_info.accessible_data:
            policy_with_dependency[cur_key].accessible_data.append(data_id)
    # Recursive case: current API has children
    # We first update the policy's effect for this API, then for all its children
    else:
        cur_key = (user_id, api)
        cur_policy_info = policy_with_dependency[cur_key]
        if data_id not in cur_policy_info.accessible_data:
            policy_with_dependency[cur_key].accessible_data.append(data_id)
        for child_api in dependency_graph[api]:
            update_policy_effect(user_id, child_api, data_id)

def initialize():
    global list_of_apis
    global list_of_dependencies
    global list_of_policies
    global list_of_uid
    global dependency_graph
    global policy_with_dependency

    # get list of APIs from DB
    api_res = database_api.get_all_apis()
    list_of_apis = list(api_res.data)

    # Initialize dependency_graph using the list of apis as keys
    for api in list_of_apis:
        dependency_graph[api] = []

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

    # get list of uids from DB
    user_res = database_api.get_all_users()
    for i in range(len(user_res.data)):
        uid = user_res.data[i].id
        list_of_uid.append(uid)

    # Initialize policy_with_dependency from dependency_graph and list_of_uid
    for uid in list_of_uid:
        for key in dependency_graph:
            cur_key = (uid, key)
            cur_accessible_data = []
            cur_odata_type = get_odata_type(key, dependency_graph)
            cur_policyInfo = PolicyInfo(cur_accessible_data, cur_odata_type)
            policy_with_dependency[cur_key] = cur_policyInfo

    # get list of policies
    policy_res = database_api.get_all_policies()
    for i in range(len(policy_res.data)):
        cur_tuple = (policy_res.data[i].user_id,
                     policy_res.data[i].api,
                     policy_res.data[i].data_id,)
        list_of_policies.append(cur_tuple)

    # From list_of_policies, fill in policy_with_dependency with policies
    for policy in list_of_policies:
        update_policy_effect(policy[0], policy[1], policy[2])

    # Check if policy_with_dependency is initialized correctly
    for key, value in policy_with_dependency.items():
        print(key)
        print(value.accessible_data)
        print(value.odata_type)

def add_new_api(api):
    global list_of_apis
    list_of_apis.append(api)

def get_all_apis():
    return list_of_apis

def add_new_api_depend(api_depend):
    global list_of_dependencies
    list_of_dependencies.append(api_depend)

def get_all_dependencies():
    return list_of_dependencies

def add_new_policy(policy):
    global list_of_policies
    # First adding the policy to the list of policies
    list_of_policies.append(policy)
    # Then update this policy's effect
    update_policy_effect(policy[0], policy[1], policy[2])

def get_all_policies():
    return list_of_policies

def get_dependency_graph():
    return dependency_graph

# For gatekeeper
def get_user_api_info(user_id, api):
    cur_key = (user_id, api)
    return policy_with_dependency[cur_key]
