import pathlib

class DSConfig:
    def __init__(self, ds_config):
        """
        Class that stores DS config variables at the time they were given
        """
        # get the trust mode for the data station
        self.trust_mode = ds_config["trust_mode"]

        # get storage path for data
        self.storage_path = ds_config["storage_path"]

        # log arguments
        self.log_in_memory_flag = ds_config["log_in_memory"]
        self.log_path = ds_config["log_path"]

        # wal arguments
        self.wal_path = ds_config["wal_path"]
        self.check_point_freq = ds_config["check_point_freq"]

        # the table_paths in dbservice.check_point
        self.table_paths = ds_config["table_paths"]

        # interceptor paths
        self.ds_storage_path = str(pathlib.Path(
            ds_config["storage_path"]).absolute())

        # EPF connector
        self.epf_path = ds_config["epf_path"]

        # development mode
        self.development_mode = ds_config["in_development_mode"]

        # OS
        self.operating_system = ds_config["operating_system"]
