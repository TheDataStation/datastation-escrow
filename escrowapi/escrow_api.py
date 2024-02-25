class EscrowAPI:
    __comp = None

    @classmethod
    def set_comp(cls, api_implementation):
        print("setting escrow api composition to: ", api_implementation)
        cls.__comp = api_implementation

    @classmethod
    def get_all_accessible_des(cls):
        """
        For functions.
        Returns all accessible data elements.

        Returns:
            A list of DataElements.
        """
        return cls.__comp.get_all_accessible_des()

    @classmethod
    def get_de_by_id(cls, de_id):
        """
        For functions.
        Returns a data element, specified by de_id

        Parameters:
            de_id: id of the DataElement to be returned.

        Returns:
            a DataElement object.
        """
        return cls.__comp.get_de_by_id(de_id)

    @classmethod
    def write_staged(cls, file_name, user_id, content):
        """
        For functions.
        Writes "content" to a file under "file_name" for user_id.
        """
        return cls.__comp.write_staged(file_name, user_id, content)

    @classmethod
    def release_staged(cls, user_id):
        """
        For API endpoints.
        For a user, releases all files in the user's staging storage.
        """
        return cls.__comp.release_staged(user_id)

    @classmethod
    def list_all_agents(cls,
                        user_id):
        """
        For API endpoints.
        Lists all agents' (ID, name) in the current instance.
        """
        return cls.__comp.list_all_agents(user_id)

    @classmethod
    def register_de(cls,
                    user_id,
                    data_name,
                    data_type,
                    access_param
                    ):
        """
        For API endpoints.
        Registers a DE in Data Station's database.

        Parameters:
            user_id: caller id (owner of the data element)
            data_name: name of the data
            data_type: type of DE. e.g: file.
            access_param: additional parameters needed for acccessing the DE

        Returns:
        A response object with the following fields:
            status: status of registering DE. 0: success, 1: failure.
            de_id: if success, a de_id is returned for this registered DE.
        """
        return cls.__comp.register_de(user_id, data_name, data_type, access_param)

    @classmethod
    def upload_de(cls,
                  user_id,
                  de_id,
                  data_in_bytes):
        """
        For API endpoints.
        Upload data in bytes corresponding to a registered DE. These bytes will be written to a file in DataStation's
        storage manager.

        Parameters:
            user_id: caller id (owner of the data element)
            de_id: id of this existing DE
            data_in_bytes: plaintext data in bytes
        """
        return cls.__comp.upload_de(user_id, de_id, data_in_bytes)

    @classmethod
    def remove_de_from_storage(cls,
                               user_id,
                               de_id):
        """
        For API endpoints.
        Remove a DE from storage. (Does not remove it from DB)
        """
        return cls.__comp.remove_de_from_storage(user_id, de_id)

    @classmethod
    def remove_de_from_db(cls,
                          user_id,
                          de_id):
        """
        For API endpoints.
        Remove a DE from DB. Also removes it from storage, if it's still in storage.
        """
        return cls.__comp.remove_de_from_db(user_id, de_id)

    @classmethod
    def list_all_des_with_src(cls, user_id):
        """
        API Endpoint.
        List IDs of all des with their source agents.

        Parameters:
            user_id: caller id
        """
        return cls.__comp.list_all_des_with_src(user_id)

    @classmethod
    def get_all_functions(cls, user_id):
        """
        For API endpoints.
        List names of all functions (those that need to access DEs). e.g. train_joint_model()
        """
        return cls.__comp.get_all_functions(user_id)

    @classmethod
    def get_function_info(cls, user_id, function_name):
        """
        For API endpoints
        Return docstring of given function.
        """
        return cls.__comp.get_function_info(user_id, function_name)

    @classmethod
    def propose_contract(cls,
                         user_id,
                         dest_agents,
                         data_elements,
                         function,
                         *args,
                         **kwargs, ):
        """
        API Endpoint.
        Propose a contract.

        Parameters:
            user_id: caller id
            dest_agents: list of user ids
            data_elements: list of data elements
            function: function
            args: input args to the template function
            kwargs: input kwargs to the template funciton

        Returns:
        A response object with the following fields:
            status: status of suggesting contract. 0: success, 1: failure.
            contract_id: (if success) id of the proposed contract
        """
        return cls.__comp.propose_contract(user_id, dest_agents, data_elements, function, *args, **kwargs)

    @classmethod
    def show_contract(cls, user_id, contract_id):
        """
        For API endpoints.
        Display the content of a contract.

        Parameters:
            user_id: caller username
            contract_id: id of the contract that the caller wants to see

        Returns:
        An object with the following fields:
            a_dest: a list of ids of the destination agents
            de: a list of ids of the data elements
            template: which template function
            args: arguments to the template function
            kwargs: kwargs to the template function
        """
        return cls.__comp.show_contract(user_id, contract_id)

    @classmethod
    def show_all_contracts_as_dest(cls, user_id):
        """
        For API endpoints.
        Display all contracts, for which caller is a destination agent.
        """
        return cls.__comp.show_all_contracts_as_dest(user_id)

    @classmethod
    def show_all_contracts_as_src(cls, user_id):
        """
        For API endpoints.
        Display all contracts, for which caller is an approval agent.
        """
        return cls.__comp.show_all_contracts_as_src(user_id)

    @classmethod
    def approve_contract(cls, user_id, contract_id):
        """
        Update a contract's status to approved (1), for source agent <user_id>.

        Parameters:
            user_id: approver id
            contract_id: id of contract

        Returns:
        A response object with the following fields:
            status: status of approving contract. 0: success, 1: failure.
        """
        return cls.__comp.approve_contract(user_id, contract_id)

    @classmethod
    def reject_contract(cls, user_id, contract_id):
        """
        Update a contract's status to rejected (-1), for source agent <user_id>.
        """
        return cls.__comp.reject_contract(user_id, contract_id)

    @classmethod
    def execute_contract(cls, user_id, contract_id):
        """
        For API endpoints.
        Execute a contract.

        Parameters:
            user_id: caller username (should be one of the dest agents)
            contract_id: id of the contract

        Returns:
            The result of executing the contract (f(P))
        """
        return cls.__comp.execute_contract(user_id, contract_id)

    @classmethod
    def upload_cmp(cls, user_id, dest_a_id, de_id, function):
        """
        Upload a new contract management policy.
        dest_a_id: 0 means any destination agent is approved
        de_id: 0 means any de is approved
        function: name of the function in the contract
        """
        return cls.__comp.upload_cmp(user_id, dest_a_id, de_id, function)

    @classmethod
    def store_kv_to_app_state(cls, key, value):
        """
        Store/Update a (key, value) pair to app state.
        """
        return cls.__comp.store_kv_to_app_state(key, value)

    @classmethod
    def load_key_from_app_state(cls, key):
        """
        Load value for key from app state.
        """
        return cls.__comp.load_key_from_app_state(key)
