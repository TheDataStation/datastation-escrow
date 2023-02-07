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


class Procedure:
    def __init__(self):
        self.registered_procedures_names = set()
        self.registered_procedures = []

    def __call__(self, _f=None):
        def decorator_func(f):
            f_name = f.__name__
            if f_name not in self.registered_procedures_names:
                self.registered_procedures.append(f)
                self.registered_procedures_names.add(f_name)

            @functools.wraps(f)
            def return_function(*args, **kwargs):
                return f(*args, **kwargs)

            return return_function
        if _f is None:
            return decorator_func
        else:
            return decorator_func(_f)

    def clear(self):
        self.registered_procedures_names = set()
        self.registered_procedures = []

# class Register:
#     def __init__(self):
#         self.registered_functions_names = set()
#         self.registered_functions = []
#         self.dependencies = dict()
#
#     def __call__(self, _f=None, *, depends_on=None):
#         def decorator_func(f):
#             f_name = f.__name__
#             if f_name not in self.registered_functions_names:
#                 self.registered_functions.append(f)
#                 self.registered_functions_names.add(f_name)
#             if depends_on is not None:
#                 self.dependencies[f.__name__] = [func.__name__ for func in depends_on]
#
#             @functools.wraps(f)
#             def return_function(*args, **kwargs):
#                 return f(*args, **kwargs)
#             return return_function
#         if _f is None:
#             return decorator_func
#         else:
#             return decorator_func(_f)
#
#     def clear(self):
#         self.registered_functions_names = set()
#         self.registered_functions = []
#         self.dependencies = dict()


function = Function()
procedure = Procedure()

def register_epf(epf_path):
    spec = importlib.util.spec_from_file_location("", epf_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

def get_procedures_names():
    procedure_names = []
    for p in procedure.registered_procedures:
        procedure_names.append(p.__name__)
    return procedure_names

def get_functions_names():
    function_names = []
    for f in function.registered_functions:
        function_names.append(f.__name__)
    return function_names

def get_registered_procedures():
    copy = [el for el in procedure.registered_procedures]
    return copy

def get_registered_functions():
    copy = [el for el in function.registered_functions]
    return copy

def get_registered_dependencies():
    return function.dependencies


# def clear_register():
#     register.clear()

# def register_connectors(connector_name, connector_module_path):
#     spec = importlib.util.spec_from_file_location(connector_name, connector_module_path)
#     module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(module)

# def __test_registration():
#     print("Any registered?")
#     funcs_reg = get_registered_functions()
#     for el in funcs_reg:
#         print(el)
#         print(el.__name__)
#         print(el.__doc__)
#
#     deps = get_registered_dependencies()
#     print(deps)


