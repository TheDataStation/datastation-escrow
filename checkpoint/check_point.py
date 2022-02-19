import pickle
from collections import namedtuple
from crypto import cryptoutils as cu

TableContent = namedtuple("TableContent",
                          "content")

class CheckPoint:

    def __init__(self, table_paths):
        self.table_paths = table_paths
        # print(self.table_paths)
