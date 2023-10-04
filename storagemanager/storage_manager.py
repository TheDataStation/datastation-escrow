from os import path, remove, makedirs, removedirs
# from common.pydantic_models.response import Response, RetrieveDataResponse

import os
import math

# TODO: Right now, we give SM the responsibility to determine how it wants to store different types of data
class StorageManager:

    def __init__(self, storage_path):
        self.storage_path = storage_path
        if not os.path.exists("SM_storage/Staging_storage"):
            os.makedirs("SM_storage/Staging_storage")
        self.staging_path = "SM_storage/Staging_storage"

    def get_dir_path(self, data_id):
        dir_path = path.join(self.storage_path, str(data_id))
        return dir_path

    def create_staging_for_user(self, user_id):
        user_staging_path = f"{self.staging_path}/{user_id}"
        if not os.path.exists(user_staging_path):
            os.makedirs(user_staging_path)

    def store(self, data_name, data_id, data, data_type):
        file_name = path.basename(data_name)
        dir_path = self.get_dir_path(data_id)
        # Specify path for file
        dst_file_path = path.join(dir_path, file_name)

        try:
            # if dir already exists, data with the same id has already been stored
            makedirs(dir_path, exist_ok=False)
            # Store the file
            iters = 3
            bytes_written = 0
            bytes_per_write = int(math.floor(len(data) / iters))
            # print(bytes_per_write)
            for j in range(iters):
                with open(dst_file_path, 'ab+') as file:
                    if j != iters - 1:
                        file.write(data[bytes_written: bytes_written + bytes_per_write])
                        bytes_written += bytes_per_write
                    else:
                        file.write(data[bytes_written:])
        except OSError as error:
            return {"status": 1, "message": f"DE id={data_id} already exists"}
        return {"status": 0, "message": "success"}

    def remove(self, data_name, data_id, data_type):
        if data_type == "file":
            file_name = path.basename(data_name)
            dir_path = self.get_dir_path(data_id)
            dst_file_path = path.join(dir_path, file_name)
            try:
                remove(dst_file_path)
                removedirs(dir_path)
            except OSError as error:
                return {"status": 1, "message": "Error removing data from storage"}
            return {"status": 0, "message": "success"}

        # Other types of data elements are not currently supported
        return {"status": 1, "message": "data type not currently supported"}

    @staticmethod
    def retrieve_data_by_id(data_type, data_access_type):
        if data_type == "file":
            f = open(data_access_type, 'rb')
            data_retrieved = f.read()
            f.close()
            return RetrieveDataResponse(status=0,
                                        message="data content retrieved successfully",
                                        data=data_retrieved,)

        # Other types of data elements are not currently supported
        return Response(status=1, message="data type not currently supported")
