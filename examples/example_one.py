import csv

from dsapplicationregistration import register
from common import ds_utils

@register
def line_count():
    """count number of lines in a file"""
    print("starting counting line numbers")
    files = ds_utils.get_all_files()
    for file in set(files):
        csv_file = open(file)
        reader = csv.reader(csv_file)
        print(len(list(reader)))

@register
def get_first_n_lines(num_lines, DE_id):
    print("Getting the first", num_lines, "lines of DE id", DE_id)
    return 0
