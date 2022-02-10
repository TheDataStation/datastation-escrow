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
# db_user_name = ds_config["postgres"]["user_name"]
# db_password = ds_config["postgres"]["password"]
# db_name = ds_config["postgres"]["database_name"]

# DATABASE_URL = "postgresql://{}:{}@localhost:5432/{}".format(db_user_name, db_password, db_name)
# DATABASE_URL = "sqlite:///./data_station.db"
DATABASE_URL = ds_config["database_url"]

#
# # engine = create_engine(
# #     DATABASE_URL, connect_args={"check_same_thread": False}
# # )
# global engine
# engine = create_engine(DATABASE_URL)
#
# # @event.listens_for(engine, "connect")
# # def connect(dbapi_connection, connection_record):
# #     connection_record.info['pid'] = os.getpid()
# #
# # @event.listens_for(engine, "checkout")
# # def checkout(dbapi_connection, connection_record, connection_proxy):
# #     pid = os.getpid()
# #     if connection_record.info['pid'] != pid:
# #         connection_record.dbapi_connection = connection_proxy.dbapi_connection = None
# #         raise exc.DisconnectionError(
# #                 "Connection record belongs to pid %s, "
# #                 "attempting to check out in pid %s" %
# #                 (connection_record.info['pid'], pid)
# #         )
#
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
#
# # The following lines are for enforcing Fk constraint
#
# def _fk_pragma_on_connect(dbapi_con, con_record):
#     dbapi_con.execute('pragma foreign_keys=ON')
#
#
# event.listen(engine, 'connect', _fk_pragma_on_connect)
#
Base = declarative_base()

