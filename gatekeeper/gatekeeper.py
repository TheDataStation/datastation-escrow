from dsapplicationregistration.dsar_core import (register_connectors,
                                                 get_names_registered_functions,
                                                 get_registered_functions,
                                                 get_registered_dependencies,)

def gatekeeper_setup(connector_name, connector_module_path):
    print("Start setting up the gatekeeper")
    register_connectors(connector_name, connector_module_path)
    print("Check registration results:")
    print(get_names_registered_functions())
    print(get_registered_functions())
    print(get_registered_dependencies())
    # TODO: Time to call DB services to register these info in the DB


def call_api(api, *args, **kwargs):
    list_of_apis = get_registered_functions()
    # TODO: add the intent-policy matching process in here
    for cur_api in list_of_apis:
        if api == cur_api.__name__:
            return cur_api(*args, **kwargs)
    return "Error"
