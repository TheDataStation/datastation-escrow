import pickle
import os
from collections import namedtuple
from crypto import cryptoutils as cu
import time
from dbservice import database_api

WriteContent = namedtuple("WriteContent",
                          "caller_id, content")

class WAL:

    def __init__(self, wal_path, check_point_freq):
        self.wal_path = wal_path
        self.check_point_freq = check_point_freq
        self.entry_counter = 0

    def log(self, caller_id, entry, key_manager):

        # Use counter to determine when we need to checkpoint the DB
        # before we actually write the wal entry
        if self.entry_counter >= self.check_point_freq:
            # First create the table snapshots
            database_api.check_point_all_tables(key_manager)
            # Then erase the existing WAL
            if os.path.exists(self.wal_path):
                os.remove(self.wal_path)
            # Then restart the counter
            self.entry_counter = 0

        # First convert content to bytes
        plain_content_in_bytes = pickle.dumps(entry)

        # Get the caller's symmetric key and encrypt
        caller_sym_key_bytes = key_manager.agents_symmetric_key[caller_id]
        caller_sym_key = cu.get_symmetric_key_from_bytes(caller_sym_key_bytes)
        cipher_content_in_bytes = caller_sym_key.encrypt(plain_content_in_bytes)

        # Create WriteContent
        write_content = WriteContent(caller_id=caller_id,
                                     content=cipher_content_in_bytes)

        # Write to WAL
        with open(self.wal_path, 'ab') as log:
            entry_to_add = pickle.dumps(write_content)
            log.write(entry_to_add)
            log.flush()
            os.fsync(log)

        # Increment counter
        self.entry_counter += 1

    def read_wal(self, key_manager):
        print("Printing contents of the write ahead log:")
        entries = self.loadall(self.wal_path)
        for cur_entry in entries:
            print("Caller ID is: ")
            print(cur_entry.caller_id)
            print("Entry content is: ")
            # Get the caller's symmetric key and decrypt
            caller_sym_key_bytes = key_manager.agents_symmetric_key[cur_entry.caller_id]
            caller_sym_key = cu.get_symmetric_key_from_bytes(caller_sym_key_bytes)
            cur_plain_content_in_bytes = caller_sym_key.decrypt(cur_entry.content)
            cur_content_object = pickle.loads(cur_plain_content_in_bytes)
            print(cur_content_object)

    def recover_db_from_wal(self, key_manager):
        print("WAL starting recovery")
        entries = self.loadall(self.wal_path)
        for cur_entry in entries:
            # Get the caller's symmetric key and decrypt
            caller_sym_key_bytes = key_manager.agents_symmetric_key[cur_entry.caller_id]
            caller_sym_key = cu.get_symmetric_key_from_bytes(caller_sym_key_bytes)
            cur_plain_content_in_bytes = caller_sym_key.decrypt(cur_entry.content)
            cur_content_object = pickle.loads(cur_plain_content_in_bytes)
            # execute the statement
            exec(cur_content_object)
        print("WAL finished recovery. Check DB for correctness.")

    @staticmethod
    def loadall(filename):
        with open(filename, "rb") as f:
            while True:
                try:
                    yield pickle.load(f)
                except EOFError:
                    break

