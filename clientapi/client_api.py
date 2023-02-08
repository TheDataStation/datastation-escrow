import os
import pickle
import time
import sys
from flask import Flask, request
from multiprocessing import Process, Event, Queue, Manager

from dbservice import database_api

from common.pydantic_models.api import API
from common.pydantic_models.user import User
from common.pydantic_models.dataset import Dataset
from common.pydantic_models.response import Response
from common.pydantic_models.policy import Policy

from userregister import user_register
from dataregister import data_register
from policybroker import policy_broker
from gatekeeper import gatekeeper
from storagemanager.storage_manager import StorageManager
from stagingstorage.staging_storage import StagingStorage
from verifiability.log import Log
import pathlib
from writeaheadlog.write_ahead_log import WAL
from crypto.key_manager import KeyManager
from crypto import cryptoutils as cu
from dbservice.database import engine
from dbservice.database_api import clear_checkpoint_table_paths
from ds import DataStation

app = Flask(__name__)

def flask_thread(port, config, app_config):
    ds = DataStation(config, app_config)

    # TODO: make ds stateless

    # create user
    @app.route("/call_api", methods=['post'])
    def call_api():
        unpickled = (request.get_data())
        # print(unpickled)
        args = pickle.loads(unpickled)
        # print("Call API Arguments are:", args)

        return pickle.dumps(ds.call_api(args['username'], args['api'], args['exec_mode'], ds, *args['args'], **args['kwargs']))

    # create user
    @app.route("/create_user", methods=['post'])
    def create_user():
        unpickled = (request.get_data())
        # print(unpickled)
        args = pickle.loads(unpickled)
        print("Received 'Create User'. User Arguments are:", args, file=sys.stdout)

        return pickle.dumps(ds.create_user(args['user'], args.get('user_sym_key', None), args.get('user_public_key', None)))

    # log in
    @app.route("/login_user")
    def login_user(username, password):
        response = user_register.login_user(username, password)
        if response.status == 0:
            return {"access_token": response.token, "token_type": "bearer"}
        else:
            # if password cannot correctly be verified, we return -1 to indicate login has failed
            return -1

    app.run(debug=False, host="localhost", port=port)
    return


class ClientAPI:

    """
    validates the login credentials of the user, then if the user is authorized
     the computation is passed to the ds class
    """
    def validate_and_get_username(token):
        # place for decryption of token
        # decrypt(token)

        # Perform authentication
        username = user_register.authenticate_user(token)

        return username

    def __init__(self,
                 config, app_config, port=8080):

        self.port = port

        self.server = Process(target=flask_thread, args=(self.port,config, app_config))
        return


    def start_server(self):
        self.server.start()



    def shut_down(self):


        print("client_api shut down complete")

if __name__ == "__main__":
    print("Client API")
