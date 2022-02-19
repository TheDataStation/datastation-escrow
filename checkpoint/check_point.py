import pickle
from collections import namedtuple
from crypto import cryptoutils as cu

TableContent = namedtuple("TableContent",
                          "content")

class CheckPoint:

    def __init__(self, table_paths):
        self.table_paths = table_paths

    def check_point_all_tables(self, key_manager):
        print("Need to check point all tables!")
