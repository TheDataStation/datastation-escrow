import os
import json
import psycopg2
import requests


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
        if self.type != "file":
            self.access_param = json.loads(self.access_param)

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
            conn_string = self.access_param["credentials"]
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            query = self.access_param["query"]
            cursor.execute(query)
            res = []
            for row in cursor:
                res.append(row)
            return res
        elif self.type == "opendata":
            api_key = self.access_param["api_key"]
            endpoint = self.access_param["endpoint"]
            headers = {'X-App-Token': api_key}
            result = requests.get(endpoint, headers=headers)
            return result.json()[0:100]
        else:
            return 0
