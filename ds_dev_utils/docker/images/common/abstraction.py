import os
import json


class DataElement:

    def __init__(self, id, name, type, access_param, enc_key=None):
        """
        For initializing an abstraction Data Element

        Parameters:
            id: id of data element
            nameP name of data element
            type: type of data element. We support a fixed set
            access_param: parameters needed to retrive this DE
            enc_key: for no trust mode, the symmetric encryption key
        """
        self.id = id
        self.name = name
        self.type = type
        self.access_param = access_param
        self.enc_key = enc_key
        if self.type == "file":
            self.access_param = os.path.join("/mnt/data_mount", str(self.id), str(self.access_param))
        else:
            self.access_param = json.loads(self.access_param)
