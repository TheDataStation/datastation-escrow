class EscrowAPI:
    __comp = None

    @classmethod
    def _set_comp(cls,api_implementation):
        print("setting escrow api composition to: ", api_implementation)
        cls.__comp = api_implementation

    @classmethod
    def get_all_accessible_des(cls):
        return cls.__comp.get_all_accessible_des()


    @classmethod
    def register_dataset(cls,
                        username,
                        data_name,
                        data_in_bytes,
                        data_type,
                        optimistic,
                        original_data_size=None,
                        ):
        return cls.__comp.register_dataset(username, data_name, data_in_bytes, data_type, optimistic, original_data_size)

    @classmethod
    def upload_policy(cls, username, user_id, api, data_id):
        return cls.__comp.upload_policy(username, user_id, api, data_id)


    @classmethod
    def suggest_share(cls, username, agents, functions, data_elements):
        return cls.__comp.suggest_share(username, agents, functions, data_elements)

    @classmethod
    def ack_data_in_share(cls, username, share_id, data_id):
        return cls.__comp.ack_data_in_share(username, share_id, data_id)
