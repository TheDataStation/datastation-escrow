import os
import json
import psycopg2

class DataElement:

    def __init__(self, id, type, access_param):
        """
        For initializing an abstraction Data Element

        Parameters:
            id: id of data element.
            type: type of data element. We support a fixed set.
            access_param: parameters needed to retrive this DE.
        """
        self.id = id
        self.type = type
        self.access_param = access_param
        if self.type == "postgres":
            self.type = json.loads(self.type)

    def get_data(self):
        """
        For getting the content of the data element.
        Right now we only support the following types:
            - file
            - postgres
        """
        if self.type == "file":
            de_path = os.path.join("/mnt/data_mount", str(self.id), str(self.access_param))
            return de_path
        elif self.type == "postgres":
            conn_string = self.access_param.credentials
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            query = self.access_param.query
            cursor.execute(query)
            res = []
            for row in cursor:
                res.append(row)
            return res
        else:
            return 0
