class EscrowAPI:
    __comp = None

    @classmethod
    def set_comp(cls, api_implementation):
        print("setting escrow api composition to: ", api_implementation)
        cls.__comp = api_implementation

    @classmethod
    def get_all_accessible_des(cls):
        """
        Used by template functions.
        Returns all accessible data elements.

        Returns:
            A list of DataElements.
        """
        return cls.__comp.get_all_accessible_des()

    @classmethod
    def get_de_by_id(cls, de_id):
        """
        Used by template functions.
        Returns a data element, specified by de_id

        Parameters:
            de_id: id of the DataElement to be returned.

        Returns:
            a DataElement object.
        """
        return cls.__comp.get_de_by_id(de_id)

    @classmethod
    def register_de(cls,
                    username,
                    data_name,
                    data_type,
                    access_param,
                    optimistic,
                    ):
        """
        Used by non-template functions.
        Registers a data element in Data Station's database.

        Parameters:
            username: the unique username identifying which user owns the dataset
            data_name: name of the data
            data_type: type of DE. e.g: file.
            access_param: additional parameters needed for acccessing the DE
            optimistic: flag to be included in optimistic data discovery

        Returns:
        A response object with the following fields:
            status: status of registering DE. 0: success, 1: failure.
            data_id: if success, a data_id is returned for this registered DE.
        """
        return cls.__comp.register_de(username, data_name, data_type, access_param, optimistic)

    @classmethod
    def upload_de(cls,
                  username,
                  data_id,
                  data_in_bytes):
        """
        Used by non-template functions.
        Upload data in bytes corresponding to a registered DE. These bytes will be written to a file in DataStation's
        storage manager.

        Parameters:
            username: the unique username identifying which user owns the dataset
            data_id: id of this existing DE
            data_in_bytes: data in bytes

        Returns:
        A response object with the following fields:
            status: status of uploading data. 0: success, 1: failure.
        """
        return cls.__comp.upload_file(username, data_id, data_in_bytes)

    @classmethod
    def list_discoverable_des(cls, username):
        return

    @classmethod
    def upload_policy(cls, username, user_id, api, data_id, share_id):
        """
        Used by non-template functions.
        Uploads a policy written by the given user to DS

        Parameters:
            username: the unique username identifying which user wrote the policy
            user_id: part of policy to upload, the user ID of the policy
            api: the api the policy refers to
            data_id: the data id the policy refers to
            share_id: to which share does this policy apply.

        Returns:
        A response object with the following fields:
            status: status of uploading policy. 0: success, 1: failure.
        """
        return cls.__comp.upload_policy(username, user_id, api, data_id, share_id)

    @classmethod
    def suggest_share(cls, username, agents, functions, data_elements):
        """
        Used by non-template functions.
        Propose a share. This leads to the creation of a share, which is just a list of policies.

        Parameters:
            username: the unique username identifying which user is calling the api
            agents: list of user ids
            functions: list of functions
            data_elements: list of data elements

        Returns:
        A response object with the following fields:
            status: status of suggesting share. 0: success, 1: failure.
        """
        return cls.__comp.suggest_share(username, agents, functions, data_elements)

    @classmethod
    def ack_data_in_share(cls, username, share_id, data_id):
        """
        Used by non-template functions.
        Updates a policy's status to ready

        Parameters:
            username: the unique username identifying which user is calling the api
            share_id: id of the share
            data_id: id of the data element

        Returns:
        A response object with the following fields:
            status: status of acknowledging share. 0: success, 1: failure.
        """
        return cls.__comp.ack_data_in_share(username, share_id, data_id)
