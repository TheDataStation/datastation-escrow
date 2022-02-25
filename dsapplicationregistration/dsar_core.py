import functools
import importlib.util


class Register:
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


register = Register()


def register_connectors(connector_name, connector_module_path):
    spec = importlib.util.spec_from_file_location(connector_name, connector_module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


def get_names_registered_functions():
    function_names = []
    for f in register.registered_functions:
        function_names.append(f.__name__)
    return function_names


def get_registered_functions():
    copy = [el for el in register.registered_functions]
    return copy


def get_registered_dependencies():
    return register.dependencies


def __test_registration():
    print("Any registered?")
    funcs_reg = get_registered_functions()
    for el in funcs_reg:
        print(el)
        print(el.__name__)
        print(el.__doc__)

    deps = get_registered_dependencies()
    print(deps)

# def validate_registration():
#     """
#     Check that functions depend on existing functions
#     Check that function dependency graph does not have cycles
#     :return:
#     """
#     # Check that all functions have been registered
#     invalid_name = True
#     reg_functions = get_names_registered_functions()
#     for key, value in register.dependencies.items():
#         if key not in reg_functions:
#             invalid_name = False
#         else:
#             for func in value:
#                 if func not in reg_functions:
#                     invalid_name = False
#     if not invalid_name:
#         return False, "Invalid names registered"
#     # Check for loops
#     valid = True
#     # TODO
#     return valid
