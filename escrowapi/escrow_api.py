class EscrowAPI:
    __comp = None

    @classmethod
    def set_comp(cls, api_implementation):
        print("setting escrow api composition to: ", api_implementation)
        cls.__comp = api_implementation

    @classmethod
    def get_all_accessible_des(cls):
        """
        For use by template functions.
        Returns all accessible data elements.

        Returns:
            A list of DataElements.
        """
        return cls.__comp.get_all_accessible_des()

    @classmethod
    def get_de_by_id(cls, de_id):
        """
        For use by template functions.
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
        Used by functions.
        Writes "content" to a file for user_id.
        """
        return cls.__comp.write_staged(file_name, user_id, content)

    @classmethod
    def release_staged(cls, user_id):
        """
        Used by API endpoints.
        For a user, releases all files in the user's staging storage.
        """
        return cls.__comp.release_staged(user_id)

    @classmethod
    def register_de(cls,
                    user_id,
                    data_name,
                    data_type,
                    access_param,
                    optimistic,
                    ):
        """
        API Endpoint.
        Registers a data element in Data Station's database.

        Parameters:
            user_id: caller id (owner of the data element)
            data_name: name of the data
            data_type: type of DE. e.g: file.
            access_param: additional parameters needed for acccessing the DE
            optimistic: flag to be included in optimistic data discovery

        Returns:
        A response object with the following fields:
            status: status of registering DE. 0: success, 1: failure.
            data_id: if success, a data_id is returned for this registered DE.
        """
        return cls.__comp.register_de(user_id, data_name, data_type, access_param, optimistic)

    @classmethod
    def upload_de(cls,
                  user_id,
                  data_id,
                  data_in_bytes):
        """
        API Endpoint.
        Upload data in bytes corresponding to a registered DE. These bytes will be written to a file in DataStation's
        storage manager.

        Parameters:
            user_id: caller id (owner of the data element)
            data_id: id of this existing DE
            data_in_bytes: data in bytes

        Returns:
        A response object with the following fields:
            status: status of uploading data. 0: success, 1: failure.
        """
        return cls.__comp.upload_de(user_id, data_id, data_in_bytes)

    @classmethod
    def list_discoverable_des(cls, user_id):
        """
        API Endpoint.
        List IDs of all des in discoverable mode.

        Parameters:
            user_id: caller id

        Returns:
        A list containing IDs of all discoverable des.
        """
        return cls.__comp.list_discoverable_des(user_id)

    @classmethod
    def upload_policy(cls, username, user_id, api, data_id, share_id):
        """
        API Endpoint.
        Uploads a policy written by the given user to DS

        Parameters:
            username: caller username
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
    def suggest_share(cls,
                      user_id,
                      dest_agents,
                      data_elements,
                      template,
                      *args,
                      **kwargs,):
        """
        API Endpoint.
        Propose a share. This leads to the creation of a share, which is just a list of policies.

        Parameters:
            user_id: caller id
            dest_agents: list of user ids
            data_elements: list of data elements
            template: template function
            args: input args to the template function
            kwargs: input kwargs to the template funciton

        Returns:
        A response object with the following fields:
            status: status of suggesting share. 0: success, 1: failure.
        """
        return cls.__comp.suggest_share(user_id, dest_agents, data_elements, template, *args, **kwargs)

    @classmethod
    def show_share(cls, user_id, share_id):
        """
        API Endpoint.
        Display the content of a share.

        Parameters:
            user_id: caller username
            share_id: id of the share that the caller wants to see

        Returns:
        An object with the following fields:
            a_dest: a list of ids of the destination agents
            de: a list of ids of the data elements
            template: which template function
            args: arguments to the template function
            kwargs: kwargs to the template function
        """
        return cls.__comp.show_share(user_id, share_id)

    @classmethod
    def approve_share(cls, user_id, share_id):
        """
        API Endpoint.
        Update a share's status to ready, for approval agent <username>.

        Parameters:
            user_id: caller username
            share_id: id of the share

        Returns:
        A response object with the following fields:
            status: status of approving share. 0: success, 1: failure.
        """
        return cls.__comp.approve_share(user_id, share_id)

    @classmethod
    def execute_share(cls, user_id, share_id):
        """
        API Endpoint.
        Execute a share.

        Parameters:
            user_id: caller username (should be one of the dest agents)
            share_id: id of the share

        Returns:
            The result of executing the share (f(P))
        """
        return cls.__comp.execute_share(user_id, share_id)
