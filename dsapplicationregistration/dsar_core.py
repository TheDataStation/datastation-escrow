import functools
import importlib.util


class Function:
    def __init__(self):
        self.registered_functions_names = set()
        self.registered_functions = []
        self.dependencies = dict()

    def __call__(self, _f=None, *, depends_on=None):
        def decorator_func(f):
            f_name = f.__name__
            if f_name not in self.registered_functions_names:
                self.registered_functions.append(f)
                self.registered_functions_names.add(f_name)
            if depends_on is not None:
                self.dependencies[f.__name__] = [func.__name__ for func in depends_on]

            @functools.wraps(f)
            def return_function(*args, **kwargs):
                return f(*args, **kwargs)
            return return_function
        if _f is None:
            return decorator_func
        else:
            return decorator_func(_f)

    def clear(self):
        self.registered_functions_names = set()
        self.registered_functions = []
        self.dependencies = dict()


class APIEndPoint:
    def __init__(self):
        self.registered_api_endpoint_names = set()
        self.registered_api_endpoint = []

    def __call__(self, _f=None):
        def decorator_func(f):
            f_name = f.__name__
            if f_name not in self.registered_api_endpoint_names:
                self.registered_api_endpoint.append(f)
                self.registered_api_endpoint_names.add(f_name)

            @functools.wraps(f)
            def return_function(*args, **kwargs):
                return f(*args, **kwargs)

            return return_function
        if _f is None:
            return decorator_func
        else:
            return decorator_func(_f)

    def clear(self):
        self.registered_api_endpoint_names = set()
        self.registered_api_endpoint = []


function = Function()
api_endpoint = APIEndPoint()

def register_epf(epf_path):
    spec = importlib.util.spec_from_file_location("", epf_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # remove_function_from_api_endpoint()

def get_api_endpoint_names():
    api_endpoint_names = []
    for api in api_endpoint.registered_api_endpoint:
        api_endpoint_names.append(api.__name__)
    return api_endpoint_names

def get_functions_names():
    function_names = []
    for f in function.registered_functions:
        function_names.append(f.__name__)
    return function_names

def get_registered_api_endpoint():
    copy = [el for el in api_endpoint.registered_api_endpoint]
    return copy

def get_registered_functions():
    copy = [el for el in function.registered_functions]
    return copy

def remove_function_from_api_endpoint():
    updated_api_endpoint = []
    updated_api_endpoint_names = []
    for a in api_endpoint.registered_api_endpoint:
        if a.__name__ not in function.registered_functions_names:
            updated_api_endpoint.append(a)
            updated_api_endpoint_names.append(a.__name__)
    api_endpoint.registered_api_endpoint = updated_api_endpoint
    api_endpoint.registered_api_endpoint_names = updated_api_endpoint_names

def get_registered_dependencies():
    return function.dependencies

def clear_function():
    function.clear()

def clear_api_endpoint():
    api_endpoint.clear()
