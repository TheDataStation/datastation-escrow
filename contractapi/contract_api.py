class ContractAPI:
    __comp = None

    class CSVDEStore:
        __comp = None

        @classmethod
        def set_comp(cls, api_implementation):
            cls.__comp = api_implementation

        # Authenticated
        @classmethod
        def write(cls, content):
            """
            Registers a new DE as a CSV file.
            Returns de_id of newly registerd DE if success.
            """
            return cls.__comp.csv_store_write(content)

        @classmethod
        def read(cls, de_id):
            """
            Returns a path to the CSV file with {de_id}.
            """
            return cls.__comp.csv_store_read(de_id)

    class ObjectDEStore:
        __comp = None

        @classmethod
        def set_comp(cls, api_implementation):
            cls.__comp = api_implementation

        # Authenticated
        @classmethod
        def write(cls, content):
            """
            Registers a new DE as an object.
            Returns de_id of newly registerd DE if success.
            """
            return cls.__comp.object_store_write(content)

        @classmethod
        def read(cls, de_id):
            """
            Returns the object with {de_id}.
            """
            return cls.__comp.object_store_read(de_id)

    @classmethod
    def set_comp(cls, api_implementation):
        print("setting escrow api composition to: ", api_implementation)
        cls.__comp = api_implementation
        cls.CSVDEStore.set_comp(api_implementation)
        cls.ObjectDEStore.set_comp(api_implementation)

    # Authenticated
    @classmethod
    def list_all_agents(cls):
        """
        For API endpoints.
        Lists all agents' (ID, name) in the current instance.
        """
        return cls.__comp.list_all_agents()

    # Authenticated
    @classmethod
    def list_all_des_with_src(cls):
        """
        API Endpoint.
        List IDs of all des with their source agents.

        Parameters:
            user_id: caller id
        """
        return cls.__comp.list_all_des_with_src()

    # Authenticated
    @classmethod
    def get_all_functions(cls):
        """
        For API endpoints.
        List names of all functions (those that need to access DEs). e.g. train_joint_model()
        """
        return cls.__comp.get_all_functions()

    # Authenticated
    @classmethod
    def get_function_info(cls, function_name):
        """
        For API endpoints
        Return docstring of given function.
        """
        return cls.__comp.get_function_info(function_name)

    # Authenticated
    @classmethod
    def propose_contract(cls,
                         dest_agents,
                         des,
                         function,
                         *args,
                         **kwargs, ):
        """
        Propose a contract.

        Parameters:
            dest_agents: list of user ids
            des: list of data elements
            function: function
            args: input args to the function
            kwargs: input kwargs to the funciton

        Returns:
        A response object with the following fields:
            status: status of suggesting contract. 0: success, 1: failure.
            contract_id: (if success) id of the proposed contract
            contract_approved: (if success) True if the proposed contract is automatically approved; False otherwise
        """
        return cls.__comp.propose_contract(dest_agents, des, function, *args, **kwargs)

    # Authenticated
    @classmethod
    def show_contract(cls, contract_id):
        """
        For API endpoints.
        Display the content of a contract.

        Parameters:
            contract_id: id of the contract that the caller wants to see

        Returns:
        An object with the following fields:
            a_dest: a list of ids of the destination agents
            de: a list of ids of the data elements
            template: which template function
            args: arguments to the template function
            kwargs: kwargs to the template function
        """
        return cls.__comp.show_contract(contract_id)

    # Authenticated
    @classmethod
    def show_my_contracts_pending_approval(cls):
        """
        For API endpoints.
        Display all contracts, for which caller is a destination agent.
        """
        return cls.__comp.show_my_contracts_pending_approval()

    # Authenticated
    @classmethod
    def show_contracts_pending_my_approval(cls):
        """
        Display all contracts that the caller has not approved yet.
        """
        return cls.__comp.show_contracts_pending_my_approval()

    # Authenticated
    @classmethod
    def approve_contract(cls, contract_id):
        """
        Update a contract's status to approved (1), for source agent <user_id>.

        Parameters:
            contract_id: id of contract

        Returns:
        A response object with the following fields:
            status: status of approving contract. 0: success, 1: failure.
        """
        return cls.__comp.approve_contract(contract_id)

    # Authenticated
    @classmethod
    def reject_contract(cls, user_id, contract_id):
        """
        Update a contract's status to rejected (-1), for source agent <user_id>.
        """
        return cls.__comp.reject_contract(user_id, contract_id)

    # # Authenticated
    # @classmethod
    # def execute_contract(cls, user_id, contract_id):
    #     """
    #     For API endpoints.
    #     Execute a contract.
    #
    #     Parameters:
    #         user_id: caller username (should be one of the dest agents)
    #         contract_id: id of the contract
    #
    #     Returns:
    #         The result of executing the contract (f(P))
    #     """
    #     return cls.__comp.execute_contract(user_id, contract_id)

    # @classmethod
    # def write_intermediate_DE(cls,
    #                           data_elements,
    #                           function,
    #                           *args,
    #                           **kwargs, ):
    #     return cls.__comp.compute_intermediate_DE(data_elements, function, *args, **kwargs)

    # Authenticated
    # TODO: the implementation of this should probably be improved
    @classmethod
    def upload_cmr(cls, dest_a_id, de_id, function):
        """
        Upload a new contract management policy.
        dest_a_id: 0 means any destination agent is approved
        de_id: 0 means any de is approved
        function: name of the function in the contract
        """
        return cls.__comp.upload_cmr(dest_a_id, de_id, function)

    # @classmethod
    # def get_contract_de_ids(cls):
    #     """
    #     For functions.
    #     Returns Ids of all DEs in the contract.
    #
    #     Returns:
    #         A list of DataElements.
    #     """
    #     return cls.__comp.get_contract_de_ids()

    @classmethod
    def store(cls, key, value):
        """
        Store/Update a (key, value) pair to app state.
        """
        return cls.__comp.store(key, value)

    @classmethod
    def load(cls, key):
        """
        Load value for key from app state.
        """
        return cls.__comp.load(key)

    @classmethod
    def get_comp(cls):
        return cls.__comp

    # @classmethod
    # def get_de_by_id(cls, de_id):
    #     """
    #     For functions.
    #     Returns a data element, specified by de_id
    #
    #     Parameters:
    #         de_id: id of the DataElement to be returned.
    #
    #     Returns:
    #         a DataElement object.
    #     """
    #     return cls.__comp.get_de_by_id(de_id)

    # @classmethod
    # def write_staged(cls, file_name, user_id, content):
    #     """
    #     For functions.
    #     Writes "content" to a file under "file_name" for user_id.
    #     """
    #     return cls.__comp.write_staged(file_name, user_id, content)
    #
    # @classmethod
    # def release_staged(cls, user_id):
    #     """
    #     For API endpoints.
    #     For a user, releases all files in the user's staging storage.
    #     """
    #     return cls.__comp.release_staged(user_id)

    # @classmethod
    # def register_de(cls,
    #                 user_id,
    #                 data_name,
    #                 data_type,
    #                 access_param
    #                 ):
    #     """
    #     For API endpoints.
    #     Registers a DE in Data Station's database.
    #
    #     Parameters:
    #         user_id: caller id (owner of the data element)
    #         data_name: name of the data
    #         data_type: type of DE. e.g: file.
    #         access_param: additional parameters needed for acccessing the DE
    #
    #     Returns:
    #     A response object with the following fields:
    #         status: status of registering DE. 0: success, 1: failure.
    #         de_id: if success, a de_id is returned for this registered DE.
    #     """
    #     return cls.__comp.register_de(user_id, data_name, data_type, access_param)
    #
    # @classmethod
    # def upload_de(cls,
    #               user_id,
    #               de_id,
    #               data_in_bytes):
    #     """
    #     For API endpoints.
    #     Upload data in bytes corresponding to a registered DE. These bytes will be written to a file in DataStation's
    #     storage manager.
    #
    #     Parameters:
    #         user_id: caller id (owner of the data element)
    #         de_id: id of this existing DE
    #         data_in_bytes: plaintext data in bytes
    #     """
    #     return cls.__comp.upload_de(user_id, de_id, data_in_bytes)

    # @classmethod
    # def remove_de_from_storage(cls,
    #                            user_id,
    #                            de_id):
    #     """
    #     For API endpoints.
    #     Remove a DE from storage. (Does not remove it from DB)
    #     """
    #     return cls.__comp.remove_de_from_storage(user_id, de_id)

    # @classmethod
    # def remove_de_from_db(cls,
    #                       user_id,
    #                       de_id):
    #     """
    #     For API endpoints.
    #     Remove a DE from DB. Also removes it from storage, if it's still in storage.
    #     """
    #     return cls.__comp.remove_de_from_db(user_id, de_id)
