from crypto import cryptoutils as cu

import pickle

class AppStateManager:

    def __init__(self, app_state_path, ds_sym_key=None):
        self.app_state_path = app_state_path
        self.ds_sym_key = ds_sym_key
        state_dict = {}
        bytes_to_write = pickle.dumps(state_dict)
        if self.ds_sym_key is not None:
            bytes_to_write = cu.encrypt_data_with_symmetric_key(bytes_to_write, self.ds_sym_key)
        with open(self.app_state_path, 'wb+') as f:
            f.write(bytes_to_write)

    def store(self, key, value):
        with open(self.app_state_path, "rb") as f:
            state_dict_bytes = f.read()
        if self.ds_sym_key is not None:
            state_dict_bytes = cu.decrypt_data_with_symmetric_key(state_dict_bytes, self.ds_sym_key)
        state_dict = pickle.loads(state_dict_bytes)
        state_dict[key] = value
        bytes_to_write = pickle.dumps(state_dict)
        if self.ds_sym_key is not None:
            bytes_to_write = cu.encrypt_data_with_symmetric_key(bytes_to_write, self.ds_sym_key)
        with open(self.app_state_path, 'wb+') as f:
            f.write(bytes_to_write)
            return 0

    def load(self, key):
        with open(self.app_state_path, "rb") as f:
            state_dict_bytes = f.read()
        if self.ds_sym_key is not None:
            state_dict_bytes = cu.decrypt_data_with_symmetric_key(state_dict_bytes, self.ds_sym_key)
        state_dict = pickle.loads(state_dict_bytes)
        if key in state_dict:
            return state_dict[key]
        return None
