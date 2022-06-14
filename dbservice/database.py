from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import os
import pathlib
from common import general_utils

ds_path = str(pathlib.Path(__file__).parent.resolve().parent)
ds_config = general_utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))

DATABASE_URL = ds_config["database_url"]

# if in no trust mode, create in memory db
if ds_config["trust_mode"] == "no_trust":
    DATABASE_URL = "sqlite://"

# on disk global engine
engine = create_engine(DATABASE_URL)

Base = declarative_base()

