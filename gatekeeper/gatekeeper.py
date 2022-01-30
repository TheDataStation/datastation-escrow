from dsapplicationregistration.dsar_core import (register_connectors,
                                                 get_names_registered_functions,
                                                 get_registered_functions,
                                                 get_registered_dependencies,)
from dbservice import database_api
from models.api import *
from models.api_dependency import *
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


def call_api(api, *args, **kwargs):
    list_of_apis = get_registered_functions()
    # TODO: add the intent-policy matching process in here
    for cur_api in list_of_apis:
        if api == cur_api.__name__:
            return cur_api(*args, **kwargs)
    return "Error"
