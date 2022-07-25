from os import path, makedirs
from common.pydantic_models.response import Response

class StagingStorage:

    def __init__(self, staging_path):
        self.staging_path = staging_path

    def get_dir_path(self, data_id):
        dir_path = path.join(self.staging_path, str(data_id))
        return dir_path

    def store(self, data_id, data):
        try:
            dir_path = self.get_dir_path(data_id)
            # Speficy path for file
            dst_file_path = path.join(dir_path, "data")
            # if dir already exists, data with the same id has already been stored
            makedirs(dir_path, exist_ok=False)
            # Store the file
            f = open(dst_file_path, 'wb')
            f.write(data)
            f.close()
            return Response(status=0,
                            message="staging data store successfully")
        except OSError as error:
            return Response(status=1,
                            message="staging data id=" + str(data_id) + "already exists")
