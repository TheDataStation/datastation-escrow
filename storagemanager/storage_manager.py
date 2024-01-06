from os import path, remove, makedirs, removedirs

import os
import math
import shutil

# TODO: Right now, we give SM the responsibility to determine how it wants to store different types of data
class StorageManager:

    def __init__(self, storage_path):
        self.storage_path = storage_path
        if not os.path.exists("SM_storage/Staging_storage"):
            os.makedirs("SM_storage/Staging_storage")
        self.staging_path = "SM_storage/Staging_storage"

    def get_dir_path(self, de_id):
        dir_path = path.join(self.storage_path, str(de_id))
        return dir_path

    def create_staging_for_user(self, user_id):
        user_staging_path = f"{self.staging_path}/{user_id}"
        if not os.path.exists(user_staging_path):
            os.makedirs(user_staging_path)

    def store(self, de_name, de_id, data, data_type):
        file_name = path.basename(de_name)
        dir_path = self.get_dir_path(de_id)
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
            return {"status": 1, "message": f"DE id={de_id} already exists"}
        return {"status": 0, "message": "success"}

    def remove_de_from_storage(self, de_id):
        dir_path = self.get_dir_path(de_id)
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            try:
                shutil.rmtree(dir_path)
            except OSError:
                return {"status": 1, "message": "Error removing DE from storage"}
            return {"status": 0, "message": "success"}

        return {"status": 0, "message": "DE not in storage"}

    @staticmethod
    def retrieve_data_by_id(data_type, data_access_type):
        if data_type == "file":
            f = open(data_access_type, 'rb')
            data_retrieved = f.read()
            f.close()
            return {"status": 0, "message": "data content retrieved successfully", "data": "data_retrieved"}

        # Other types of data elements are not currently supported
        return {"status": 1, "message": "DE type not currently supported"}
