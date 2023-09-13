import os
import pickle
from crypto import cryptoutils


class EscrowAPIDocker:

    def __init__(self, accessible_de, agents_symmetric_key):
        self.accessible_de = accessible_de
        self.agents_symmetric_key = agents_symmetric_key
        for cur_de in self.accessible_de:
            if cur_de.type == "file":
                cur_de.access_param = os.path.join("/mnt/data_mount/", cur_de.access_param)
        print(self.agents_symmetric_key)

    def get_all_accessible_des(self):
        return self.accessible_de

    def get_de_by_id(self, de_id):
        for de in self.accessible_de:
            if de.id == de_id:
                return de

    def write_staged(self, file_name, user_id, content):
        """
        first pickle the content to bytes, then encrypt it using the user_id's sym key
        then write to file
        """
        # with open("/mnt/data_mount/hello.csv", 'w+') as csvfile:
        #     csvfile.write("hello")
        pk_bytes = pickle.dumps(content)
        user_sym_key = self.agents_symmetric_key[user_id]
        enc_bytes = cryptoutils.encrypt_data_with_symmetric_key(pk_bytes, user_sym_key)
        # TODO: write to file
