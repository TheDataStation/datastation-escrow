import os
from os import path
import pickle
from crypto import cryptoutils


class EscrowAPIDocker:

    def __init__(self, accessible_de, agents_symmetric_key):
        self.accessible_de = accessible_de
        self.agents_symmetric_key = agents_symmetric_key
        self.app_state_path = "/mnt/data_mount/app_state.pkl"
        self.derived_des_to_create = []
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

    # Write to ObjectDEStore.
    # We just keep track of how many intermediate DEs we are writing. Their src DE will be all DEs accessed.
    # GK will take care of the rest.
    def object_store_write(self, content):
        derived_de = ("object", content)
        self.derived_des_to_create.append(derived_de)

    # State saving/loading
    # Decryption should have been done by interceptor already
    def save(self, key, value):
        with open(self.app_state_path, "rb") as f:
            state_dict_bytes = f.read()
        state_dict = pickle.loads(state_dict_bytes)
        state_dict[key] = value
        bytes_to_write = pickle.dumps(state_dict)
        with open("/mnt/data_mount/app_state.pkl", 'wb+') as f:
            f.write(bytes_to_write)
            return 0

    def load(self, key):
        with open(self.app_state_path, "rb") as f:
            state_dict_bytes = f.read()
        state_dict = pickle.loads(state_dict_bytes)
        if key in state_dict:
            return state_dict[key]
        return None

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
