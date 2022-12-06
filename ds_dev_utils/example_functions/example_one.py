from dsapplicationregistration.dsar_core import register
from common import ds_utils
import csv

# @register
# def hello_world():
#     print("hello world from within function")
#     return "Hello World"

# @register
# def read_file(filename):
#     f = open(filename, "r")
#     return f.read()


@register
def line_count():
    """count number of lines in a file"""
    print("starting counting line numbers")
    files = ds_utils.get_all_files()
    res = []
    for file in set(files):
        csv_file = open(file)
        reader = csv.reader(csv_file)
        res.append(len(list(reader)))
    return files
    return res

@register
def get_first_n_lines(num_lines, DE_id):
    print("Getting the first", num_lines, "lines of DE id", DE_id)
    files = ds_utils.get_specified_files(DE_id)
    res = []
    for file in set(files):
        csv_file = open(file)
        reader = csv.reader(csv_file)
        for i in range(num_lines):
            res.append(list(reader)[i])
    return res
