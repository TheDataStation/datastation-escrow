from dbservice import database_api


# Helper function to get all ancestors of an api from dependency graph (including itself)
def get_all_ancestors(api, d_graph, cur_ancestors):
    if d_graph[api]:
        cur_ancestors += d_graph[api]
        for parent_api in d_graph[api]:
            get_all_ancestors(parent_api, d_graph, cur_ancestors)


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
