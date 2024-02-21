from os import path, remove, makedirs, removedirs

import os


class AppStateManager:

    def __init__(self, app_state_path):
        self.app_state_path = app_state_path

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