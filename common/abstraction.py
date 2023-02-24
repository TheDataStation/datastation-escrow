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
        else:
            return 0


if __name__ == "__main__":
    de_one = DataElement(1, "file", "a_cool_file")
    data_one = de_one.get_data()
    de_access_param_obj = {"credentials": "host='psql-mock-database-cloud.postgres.database.azure.com' "
                                          "dbname='booking1677197406169yivvxvruhvmbvqzh' "
                                          "user='gyhksqubzvuzcrdbhovifciu@psql-mock-database-cloud' "
                                          "password='mbaqbkpppcmhsdbqukeutann'",
                           "query": "SELECT * FROM bookings"}
    de_access_param_2 = json.dumps(de_access_param_obj)
    de_two = DataElement(2, "postgres", de_access_param_2)
    data_two = de_two.get_data()
    print(data_one)
    print(data_two)
