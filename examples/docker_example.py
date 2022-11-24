from dsar_core import register


@register
def hello_world():
    print("hello world from within function")
    return "Hello World"

@register
def read_file(filename):
    f = open(filename, "r")
    return f.read()