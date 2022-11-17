from dsar_core import (register_connectors,
                        get_names_registered_functions,
                        get_registered_functions,
                        get_registered_dependencies, )
import os
import argparse
import pickle

def load_connectors(connector_dir):
    """
    looks into the connector_dir directory and loops through the files in it,
     registering each of them.

    Parameters:
     connector directory, a filepath

    Returns:
     Nothing
    """
    for filename in os.listdir(connector_dir):
        file = os.path.join(connector_dir, filename)
        if os.path.isfile(file):
            # print(file + " is file")

            register_connectors(filename, file)

def unpickle(function_name):
    filename = "pickled/pickled_" + function_name
    with open(filename,'rb') as f:
        arg_dict = pickle.load(f)
    return arg_dict

def run_function(function_name, *args, **kwargs):
    """
    runs the function from the function name and provided pickle file for inputs

    Parameters:
     function name

    Returns: function return value
    """

    list_of_apis = get_registered_functions()
    # print("list_of_apis:", list_of_apis)
    for cur_api in list_of_apis:
        if function_name == cur_api.__name__:
            # print("call", function_name)
            result = cur_api(*args, **kwargs)
            return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run functions within container.')
    parser.add_argument('function', action='store', type=str, help='The function name to parse.')
    args = parser.parse_args()

    connector_dir = "/usr/src/ds/functions"
    load_connectors(connector_dir)
    arg_dict = unpickle(args.function)
    res = run_function(args.function,*arg_dict["args"], **arg_dict["kwargs"])
    print(res)