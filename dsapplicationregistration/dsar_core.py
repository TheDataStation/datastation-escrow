import functools


class Expose:
    def __init__(self):
        self.registered_functions = []
        self.dependencies = dict()

    def __call__(self, _f=None, *, depends_on=None):
        def decorator_func(f):
            self.registered_functions.append(f)
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


expose = Expose()


def validate_registration():
    """
    Check that functions depend on existing functions
    Check that function dependency graph does not have cycles
    :return:
    """
    # Check that all functions have been registered
    invalid_name = True
    reg_functions = get_names_registered_functions()
    for key, value in expose.dependencies.items():
        if key not in reg_functions:
            invalid_name = False
        else:
            for func in value:
                if func not in reg_functions:
                    invalid_name = False
    if not invalid_name:
        return False, "Invalid names registered"
    # Check for loops
    valid = True
    # TODO
    return valid


def get_names_registered_functions():
    function_names = []
    for f in expose.registered_functions:
        function_names.append(f.__name__)
    return function_names


def get_registered_functions():
    copy = [el for el in expose.registered_functions]
    return copy


def get_registered_dependencies():
    return expose.dependencies


if __name__ == "__main__":
    print("Data Station application registration CORE")

    from dsapplicationregistration.example_registration import *
    from dsapplicationregistration.example_registration2 import *

    # @expose
    # def test1(a: int) -> int:
    #     """test 1 increments a number"""
    #     a += 1
    #     return a
    #
    #
    # @expose
    # def test2(b: str = "None", c: str = "None") -> str:
    #     """test2 appends aa to a string"""
    #     b = b + c
    #     return b

    # c = test2("a", "b")
    # print("THIS: " + str(c))

    # print(test2.__name__)

    # preprocess("path")
    #
    # train(33)

    print("Any registered?")
    funcs_reg = get_registered_functions()
    for el in funcs_reg:
        print(el)
        print(el.__name__)
        print(el.__doc__)

    # input = 'preprocess'
    #
    # for el in funcs_reg:
    #     if el.__name__ == input:
    #         print("Calling function")
    #         el.__call__("hi")

    deps = get_registered_dependencies()
    print(deps)
