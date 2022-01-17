import functools


class Expose:
    def __init__(self):
        self.registered_functions = []

    def __call__(self, f):
        self.registered_functions.append(f)

        @functools.wraps(f)
        def return_function(*args, **kwargs):
            return f(*args, **kwargs)
        return return_function


expose = Expose()


def get_names_registered_functions():
    function_names = []
    for f in expose.registered_functions:
        function_names.append(f.__name__)
    return function_names


def get_registered_functions():
    copy = [el for el in expose.registered_functions]
    return copy


if __name__ == "__main__":
    print("Data Station application registration CORE")

    from dsapplicationregistration.example_registration import *
    from dsapplicationregistration.example_registration2 import *

    @expose
    def test1(a: int) -> int:
        """test 1 increments a number"""
        a += 1
        return a


    @expose
    def test2(b: str = "None", c: str = "None") -> str:
        """test2 appends aa to a string"""
        b = b + c
        return b

    # c = test2("a", "b")
    # print("THIS: " + str(c))

    print(test2.__name__)

    # preprocess("path")
    #
    # train(33)

    print("Any registered?")
    funcs_reg = get_registered_functions()
    for el in funcs_reg:
        print(el)
        print(el.__name__)
        print(el.__doc__)
