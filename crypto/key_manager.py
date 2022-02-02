from crypto import cryptoutils as cu


class KeyManager:
    """
    The KeyManager class stores all keys necessary for DS operation. These include the private-public key pair that DS
    uses to authenticate itself and sign outgoing messages, as well as all symmetric keys and public keys from agents.
    """

    def __init__(self, config):
        """

        :param config:
        """
        # Own keys
        self.ds_private_key = None
        self.ds_public_key = None
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
        self.ds_private_key = private_key
        self.ds_public_key = public_key

    def store_agent_symmetric_key(self):
        """
        All agent keys are signed with DS's public key. This function 1) decrypts the agent's key 2) stores it in memory.
        The DS loses access to the agent's key if restarted
        :return:
        """

        return

    def store_agent_public_key(self):
        """
        Stores agent's public key
        :return:
        """
        return


if __name__ == "__main__":
    print("Data Station's Key Manager")
