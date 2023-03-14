class EscrowAPI:
    __comp = None

    @classmethod
    def set_comp(cls, api_implementation):
        print("setting escrow api composition to: ", api_implementation)
        cls.__comp = api_implementation

    @classmethod
    def get_all_accessible_des(cls):
        return cls.__comp.get_all_accessible_des()

    @classmethod
    def get_de_by_id(cls, de_id):
        return cls.__comp.get_de_by_id(de_id)

    @classmethod
    def register_data(cls,
                      username,
                      data_name,
                      data_type,
                      access_param,
                      optimistic,
                      ):
        """
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
        return cls.__comp.register_data(username, data_name, data_type, access_param, optimistic)

    @classmethod
    def upload_data(cls,
                    username,
                    data_id,
                    data_in_bytes):
        """
        Upload data in bytes corresponding to a registered DE. These bytes will be written to a file
        in DataStation's storage manager.

        Parameters:
            username: the unique username identifying which user owns the dataset
            data_id: id of this existing DE
            data_in_bytes: daat in bytes

        Returns:
        A response object with the following fields:
            status: status of uploading data. 0: success, 1: failure.
        """
        return cls.__comp.upload_file(username, data_id, data_in_bytes)

    @classmethod
    def upload_policy(cls, username, user_id, api, data_id):
        return cls.__comp.upload_policy(username, user_id, api, data_id)

    @classmethod
    def suggest_share(cls, username, agents, functions, data_elements):
        return cls.__comp.suggest_share(username, agents, functions, data_elements)

    @classmethod
    def ack_data_in_share(cls, username, share_id, data_id):
        return cls.__comp.ack_data_in_share(username, share_id, data_id)
