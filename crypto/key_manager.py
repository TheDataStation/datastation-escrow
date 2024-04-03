from crypto import cryptoutils as cu
from cryptography.hazmat.primitives import serialization
from cryptography.fernet import Fernet

class KeyManager:
    """
    The KeyManager class stores all keys necessary for DS operation. These include the private-public key pair that DS
    uses to authenticate itself and sign outgoing messages, as well as all symmetric keys and public keys from agents.
    """

    def __init__(self):
        """
        """
        # Own keys
        self.ds_private_key = None
        self.ds_public_key = None
        self.ds_symmetric_key = None
        self.initialize_data_station_keys()

        # Agents' keys
        self.agents_symmetric_key = dict()
        self.agents_public_key = dict()

    def initialize_data_station_keys(self):
        """
        Creates a public-private key pair that remains stored in memory.
        This operation is recreated every time the Data Station process is initialized. Hence, every time the DS starts
        it creates and uses a different pair of public-private keys
        :return:
        """
        # Generate public private key pair at runtime and store them in memory
        private_key, public_key = cu.generate_private_public_key_pair()
        ds_symmetric_key = Fernet.generate_key()
        # self.ds_private_key = private_key
        # self.ds_public_key = public_key
        # Load private key from file
        with open('private_key.pem', 'rb') as f:
            private_pem = f.read()
            self.ds_private_key = serialization.load_pem_private_key(
                private_pem,
                password=None
            )
        # Load public key from file
        with open('public_key.pem', 'rb') as f:
            public_pem = f.read()
            self.ds_public_key = serialization.load_pem_public_key(public_pem)
        # Store DS's own symmetric key
        with open('symmetric_key.key', 'rb') as f:
            ds_symmetric_key = f.read()
        cipher_sym_key = cu.encrypt_data_with_public_key(ds_symmetric_key, self.ds_public_key)
        # We use an ID of 0 to represent DS's own symmetric key
        self.store_agent_symmetric_key(0, cipher_sym_key)

    def store_agent_symmetric_key(self, agent_id, ciphertext_symmetric_key):
        """
        All agent keys are signed with DS's public key. This function 1) decrypts the agent's key 2) stores it in memory.
        The DS loses access to the agent's key if restarted
        :return:
        """
        symmetric_key = cu.decrypt_data_with_private_key(ciphertext_symmetric_key, self.ds_private_key)
        self.agents_symmetric_key[agent_id] = symmetric_key

    def store_agent_public_key(self, agent_id, public_key):
        """
        Stores agent's public key
        :return:
        """
        self.agents_public_key[agent_id] = public_key

    def get_agent_symmetric_key(self, agent_id):
        return self.agents_symmetric_key[agent_id]


if __name__ == "__main__":
    print("Data Station's Key Manager")
