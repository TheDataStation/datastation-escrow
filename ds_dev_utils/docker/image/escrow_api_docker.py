import os
from os import path
import pickle
from crypto import cryptoutils


class EscrowAPIDocker:

    def __init__(self, accessible_de, agents_symmetric_key):
        self.accessible_de = accessible_de
        self.agents_symmetric_key = agents_symmetric_key
        # for cur_de in self.accessible_de:
        #     if cur_de.type == "file":
        #         cur_de.access_param = os.path.join("/mnt/data_mount/", cur_de.access_param)
        print(self.agents_symmetric_key)

    # Return path to the CSV file
    def csv_store_read(self, de_id):
        dir_path = path.join("/mnt/data_mount", str(de_id))
        dst_file_path = path.join(dir_path, path.basename(f"{de_id}.csv"))
        return dst_file_path

    # Return object stored
    def object_store_read(self, de_id):
        # Should just be reading the bytes and load it to an object then return...Interceptor will take care of rest
        pass

    # Write to ObjectDEStore
    # Likely, this can only be done when we return to the Gatekeeper
    def object_store_write(self, content):
        ...

    # State saving/loading
    def save(self):
        ...

    def load(self):
        ...

    # def get_all_accessible_des(self):
    #     return self.accessible_de
    #
    # def get_de_by_id(self, de_id):
    #     for de in self.accessible_de:
    #         if de.id == de_id:
    #             return de

    # def write_staged(self, file_name, user_id, content):
    #     """
    #     first pickle the content to bytes, then encrypt it using the user_id's sym key
    #     then write to file
    #     """
    #     bytes_to_write = pickle.dumps(content)
    #     if self.agents_symmetric_key:
    #         user_sym_key = self.agents_symmetric_key[user_id]
    #         bytes_to_write = cryptoutils.encrypt_data_with_symmetric_key(bytes_to_write, user_sym_key)
    #     with open(f"/mnt/data_mount/Staging_storage/{user_id}/{file_name}", 'wb+') as f:
    #         f.write(bytes_to_write)
