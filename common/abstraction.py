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
            self.access_param = os.path.join(str(self.id), str(self.access_param))
        else:
            self.access_param = json.loads(self.access_param)

    # def get_data(self):
    #     """
    #     For getting the content of the data element.
    #     Right now we only support the following types:
    #         - file
    #         - postgres
    #     """
    #     if self.type == "file":
    #         de_path = os.path.join("/mnt/data_mount", str(self.id), str(self.access_param))
    #         return de_path
    #     elif self.type == "postgres":
    #         conn_string = self.access_param["credentials"]
    #         conn = psycopg2.connect(conn_string)
    #         cursor = conn.cursor()
    #         query = self.access_param["query"]
    #         cursor.execute(query)
    #         res = []
    #         for row in cursor:
    #             res.append(row)
    #         return res
    #     elif self.type == "opendata":
    #         api_key = self.access_param["api_key"]
    #         endpoint = self.access_param["endpoint"]
    #         headers = {'X-App-Token': api_key}
    #         result = requests.get(endpoint, headers=headers)
    #         return result.json()[0:100]
    #     else:
    #         return 0


# if __name__ == "__main__":
#     de_one = DataElement(1, "a_cool_file", "file", "a_cool_file")
#     data_one = de_one.get_data()
#     de_access_param_obj_2 = {"credentials": "host='psql-mock-database-cloud.postgres.database.azure.com' "
#                                             "dbname='booking1677197406169yivvxvruhvmbvqzh' "
#                                             "user='gyhksqubzvuzcrdbhovifciu@psql-mock-database-cloud' "
#                                             "password='mbaqbkpppcmhsdbqukeutann'",
#                              "query": "SELECT * FROM bookings"}
#     de_access_param_2 = json.dumps(de_access_param_obj_2)
#     de_two = DataElement(2, "postgres-table", "postgres", de_access_param_2)
#     data_two = de_two.get_data()
#     with open("integration_new/test_files/credentials/nyc.txt", 'r') as file:
#         api_key = file.read()
#     de_access_param_obj_3 = {"api_key": api_key,
#                              "endpoint": "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"}
#     de_access_param_3 = json.dumps(de_access_param_obj_3)
#     de_three = DataElement(3, "job_application", "opendata", de_access_param_3)
#     data_three = de_three.get_data()
#     # print(data_one)
#     # print(data_two)
#     print(data_three)
