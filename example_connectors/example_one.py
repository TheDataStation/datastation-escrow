import csv

from dsapplicationregistration import register


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
