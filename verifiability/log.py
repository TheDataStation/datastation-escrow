import pickle
from enum import Enum
from collections import namedtuple


class Log:
    """
    Definition of log entries. Each has a log_entry_type that determines the meaning of this entry. log_entry_types are
    declared in an Enum type.
    """
    IntentDefiniteDES = namedtuple('IntentFiniteDES', 'log_entry_type, agent_id, api, list_DES')
    IntentIndefiniteDES = namedtuple('IntentIndefiniteDES', 'log_entry_type, agent_id, api')
    IntentPolicyMatch = namedtuple('IntentPolicyMatch', 'log_entry_type, agent_id, api, list_DES')
    IntentPolicyMismatch = namedtuple('IntentPolicyMismatch', 'log_entry_type, agent_id, api, list_DES')
    GrantFExecution = namedtuple('GrantFExecution', 'log_entry_type, agent_id, api')

    """
    An internal enum class to declare the log entry types
    """
    class LogEntryType(Enum):
        # Indicates an intent with an a priori known list of DEs
        INTENT_DEFINITE_DES = 1
        # Indicates an intent with an a priori not known list of DEs
        INTENT_INDEFINITE_DES = 2
        # Indicates that an intent matched an existing policy
        INTENT_POLICY_MATCH = 3
        # Indicates that an intent did not match an existing policy
        INTENT_POLICY_MISMATCH = 4
        # Indicates that a function was granted permission to execute
        GRANT_F_EXECUTION = 5

    def __init__(self, in_memory=True):
        # variable to indicate whether this log is in-memory only
        self.in_memory = in_memory
        # Option for in-memory only log
        if in_memory:
            self.log = []
        else:
            # Initialize storage
            # TODO: config
            pass

    def log_intent_definite(self, agent_id: int, api: str, list_des: [int]):
        entry = self.IntentDefiniteDES(log_entry_type=self.LogEntryType.INTENT_DEFINITE_DES,
                                       agent_id=agent_id,
                                       api=api,
                                       list_DES=list_des)
        self._log(entry)

    def log_intent_indefinite(self, agent_id: int, api: str):
        entry = self.IntentIndefiniteDES(log_entry_type=self.LogEntryType.INTENT_INDEFINITE_DES,
                                         agent_id=agent_id,
                                         api=api)
        self._log(entry)

    def log_intent_policy_match(self, agent_id: int, api: str, list_des: [int]):
        entry = self.IntentPolicyMatch(log_entry_type=self.LogEntryType.INTENT_POLICY_MATCH,
                                       agent_id=agent_id,
                                       api=api,
                                       list_DES=list_des)
        self._log(entry)

    def log_intent_policy_mismatch(self, agent_id: int, api: str, list_des: [int]):
        entry = self.IntentPolicyMismatch(log_entry_type=self.LogEntryType.INTENT_POLICY_MISMATCH,
                                          agent_id=agent_id,
                                          api=api,
                                          list_DES=list_des)
        self._log(entry)

    def log_grant_f_execution(self, agent_id, api):
        entry = self.GrantFExecution(log_entry_type=self.LogEntryType.GRANT_F_EXECUTION,
                                     agent_id=agent_id,
                                     api=api)
        self._log(entry)

    def _log(self, entry):
        if self.in_memory:
            self.log.append(entry)
        else:
            # obtain bytes
            entry_bytes = pickle.dumps(entry)
            len_entry_types = len(entry_bytes)
            # TODO: store these bytes
            return

    def print_log(self):
        print("Printing contents of the log:")
        print(self.log)


if __name__ == "__main__":
    print("Verifiable Log")
