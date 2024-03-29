import pickle


class AppStateManager:

    def __init__(self, app_state_path):
        self.app_state_path = app_state_path
        state_dict = {}
        with open(self.app_state_path, 'wb+') as f:
            f.write(pickle.dumps(state_dict))

    def store(self, key, value):
        with open(self.app_state_path, "rb") as f:
            state_dict = pickle.load(f)
        state_dict[key] = value
        with open(self.app_state_path, 'wb+') as f:
            f.write(pickle.dumps(state_dict))
            return 0

    def load(self, key):
        with open(self.app_state_path, "rb") as f:
            state_dict = pickle.load(f)
            return state_dict[key]
