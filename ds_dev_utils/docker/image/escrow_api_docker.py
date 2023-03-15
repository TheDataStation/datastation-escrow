import os


class EscrowAPIDocker:

    def __init__(self, accessible_de):
        self.accessible_de = accessible_de
        for cur_de in self.accessible_de:
            if cur_de.type == "file":
                cur_de.access_param = os.path.join("/mnt/data_mount/", cur_de.access_param)

    def get_all_accessible_des(self):
        return self.accessible_de

    def get_de_by_id(self, de_id):
        for de in self.accessible_de:
            if de.id == de_id:
                return de