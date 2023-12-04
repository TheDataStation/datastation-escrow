from dbservice import database_api
from dsapplicationregistration.dsar_core import get_registered_functions

def get_all_functions():
    return database_api.get_all_functions()

def get_function_info(function_name):
    f_names_resp = database_api.get_all_functions()
    if f_names_resp["status"] == 1:
        return f_names_resp
    f_names = f_names_resp["data"]
    if function_name not in f_names:
        return {"status": 1, "message": "not a registered function"}
    functions = get_registered_functions()
    for f in functions:
        if f.__name__ == function_name:
            return f.__doc__
