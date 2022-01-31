from os.path import isfile
from os import path, remove, makedirs, removedirs
from models.response import *

# TODO: Right now, we give SM the responsibility to determine how it wants to store different types of data
class StorageManager:

    def __init__(self, storage_path):
        self.storage_path = storage_path

    def get_dir_path(self, data_id):
        dir_path = path.join(self.storage_path, str(data_id))
        return dir_path

    def store(self, data_name, data_id, data, data_type):
        if data_type == "file":
            file_name = path.basename(data_name)
            dir_path = self.get_dir_path(data_id)
            # Speficy path for file
            dst_file_path = path.join(dir_path, file_name)

            try:
                # if dir already exists, data with the same id has already been stored
                makedirs(dir_path, exist_ok=False)
                # Store the file
                f = open(dst_file_path, 'wb')
                f.write(data)
                f.close()
            except OSError as error:
                return Response(status=1,
                                message="data id=" + str(data_id) + "already exists")
            return StoreDataResponse(status=0,
                                     message="success",
                                     access_type=dst_file_path,)

        # Other types of data elements are not currently supported
        return Response(status=1, message="data type not currently supported")

    def remove(self, data_name, data_id, data_type):
        if data_type == "file":
            file_name = path.basename(data_name)
            dir_path = self.get_dir_path(data_id)
            dst_file_path = path.join(dir_path, file_name)
            try:
                remove(dst_file_path)
                removedirs(dir_path)
            except OSError as error:
                return Response(status=1,
                                message="Error removing data from storage")
            return Response(status=0, message="success")

        # Other types of data elements are not currently supported
        return Response(status=1, message="data type not currently supported")
