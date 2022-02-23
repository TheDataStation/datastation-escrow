import pickle
import os
from collections import namedtuple
from crypto import cryptoutils as cu

MatchContent = namedtuple("MatchContent",
                          "status, api, accessed_DE")
MismatchContent = namedtuple("MismatchContent",
                             "status, api, accessed_DE, accessible_DE_by_policy")
IntentPolicyMatch = namedtuple('IntentPolicyMatch',
                               'caller_id, content')
IntentPolicyMismatch = namedtuple('IntentPolicyMismatch',
                                  'caller_id, content')

class Log:

    def __init__(self, in_memory_flag, log_path, trust_mode):
        # variable to indicate whether this log is in-memory only
        self.in_memory = in_memory_flag
        # variable to indicate whether this log should be encrypted
        if trust_mode == "full_trust":
            self.encrypted = False
        else:
            self.encrypted = True
        # Option for in-memory only log
        if self.in_memory:
            self.log = []
        else:
            # Initialize storage
            self.log_path = log_path

    def log_intent_policy_match(self,
                                caller_id: int,
                                api: str,
                                accessed_DE: [int],
                                key_manager,):
        match_content = MatchContent(status=True,
                                     api=api,
                                     accessed_DE=accessed_DE,)
        log_entry = IntentPolicyMatch(caller_id=caller_id, content=match_content)
        self._log(log_entry, key_manager)

    def log_intent_policy_mismatch(self,
                                   caller_id: int,
                                   api: str,
                                   accessed_DE: [int],
                                   accessible_DE_by_policy: [int],
                                   key_manager,):
        mismatch_content = MismatchContent(status=False,
                                           api=api,
                                           accessed_DE=accessed_DE,
                                           accessible_DE_by_policy=accessible_DE_by_policy,)
        log_entry = IntentPolicyMismatch(caller_id=caller_id,
                                         content=mismatch_content)
        self._log(log_entry, key_manager)

    def _log(self, entry, key_manager):
        # In memory mode: since memory is always encrypted, we just append
        if self.in_memory:
            self.log.append(entry)
        else:
            # case 1: durable, non-encrypted log: write directly
            if not self.encrypted:
                with open(self.log_path, 'ab') as log:
                    entry_to_add = pickle.dumps(entry)
                    log.write(entry_to_add)
                    log.flush()
                    os.fsync(log)
            # case 2: durable, encrypted log: need to encrypte the content part
            # note: we still keep the caller ID field as plaintext
            else:
                with open(self.log_path, 'ab') as log:
                    # Look at plaintext fields
                    # print(entry.caller_id)
                    # print(entry.content)
                    # Now let's try converting entry.content to bytes
                    plain_content_in_bytes = pickle.dumps(entry.content)

                    # Get the caller's symmetric key and encrypt
                    caller_sym_key_bytes = key_manager.agents_symmetric_key[entry.caller_id]
                    caller_sym_key = cu.get_symmetric_key_from_bytes(caller_sym_key_bytes)
                    cipher_content_in_bytes = caller_sym_key.encrypt(plain_content_in_bytes)

                    # print(cipher_content_in_bytes)
                    # Let create the new log entry
                    if type(entry).__name__ == "IntentPolicyMatch":
                        encrypted_entry = IntentPolicyMatch(caller_id=entry.caller_id,
                                                            content=cipher_content_in_bytes,)
                    else:
                        encrypted_entry = IntentPolicyMismatch(caller_id=entry.caller_id,
                                                               content=cipher_content_in_bytes,)
                    encrypted_entry_in_bytes = pickle.dumps(encrypted_entry)
                    log.write(encrypted_entry_in_bytes)

    def read_full_log(self, key_manager):
        print("Printing contents of the log:")
        # Case 1: log is in-memory
        if self.in_memory:
            for cur_entry in self.log:
                print("Caller ID is: ")
                print(cur_entry.caller_id)
                print("Entry content is: ")
                print(cur_entry.content)
        # Case 2: log is on disk
        else:
            entries = self.loadall(self.log_path)
            for cur_entry in entries:
                print("Caller ID is: ")
                print(cur_entry.caller_id)
                print("Entry content is: ")
                # Case 2.1: log is in plaintext
                if not self.encrypted:
                    print(cur_entry.content)
                # Case 2.2: log is encrypted (no_trust mode)
                else:
                    # Get the caller's symmetric key and decrypt
                    caller_sym_key_bytes = key_manager.agents_symmetric_key[cur_entry.caller_id]
                    caller_sym_key = cu.get_symmetric_key_from_bytes(caller_sym_key_bytes)
                    cur_plain_content_in_bytes = caller_sym_key.decrypt(cur_entry.content)
                    cur_content_object = pickle.loads(cur_plain_content_in_bytes)
                    print(cur_content_object)

    @staticmethod
    def loadall(filename):
        with open(filename, "rb") as f:
            while True:
                try:
                    yield pickle.load(f)
                except EOFError:
                    break
