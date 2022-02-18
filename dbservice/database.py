from sqlalchemy import create_engine, exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event
from sqlalchemy.pool import NullPool
import os
import pathlib
from common import utils

ds_path = str(pathlib.Path(__file__).parent.resolve().parent)
ds_config = utils.parse_config(os.path.join(ds_path, "data_station_config.yaml"))

DATABASE_URL = ds_config["database_url"]

# global engine
engine = create_engine(DATABASE_URL)

# # TODO: start from here tomorrow: testing out in memory DB from here
# engine = create_engine('sqlite://')

Base = declarative_base()

