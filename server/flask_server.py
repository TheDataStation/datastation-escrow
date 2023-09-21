import os
import pickle
import sys
import argparse
import signal
import shutil
from flask import Flask, request

from main import initialize_system
from userregister import user_register

app = Flask(__name__)


# call api
@app.route("/call_api", methods=['post'])
def call_api():
    """
    Calls the API specified by a pickled dict-like structure. The dict must contain:
        username: the user that is calling this api
        api: the api to be called
        args: arguments for the api being called
        kwargs: arguments for the api being called
    """
    unpickled = (request.get_data())
    # print(unpickled)
    args = pickle.loads(unpickled)
    # print("Call API Arguments are:", args)
    # enqueue(api_call(data))

    return pickle.dumps(ds.call_api(args['username'], args['api'], *args['args'], **args['kwargs']))


# create user
@app.route("/create_user", methods=['post'])
def create_user():
    """
    Creates a user from a pickled dict-like structure. The dict must contain:
        username: the user that is calling this api
        password: password
        user_sym_key (optional): symmetric key for user
        user_public_key (optional): public key for user
    """

    unpickled = (request.get_data())
    # print(unpickled)
    args = pickle.loads(unpickled)
    print("Received 'Create User'. User Arguments are:", args, file=sys.stdout)

    return pickle.dumps(ds.create_user(args['username'], args['password'],
                                       args.get('user_sym_key', None),
                                       args.get('user_public_key', None)))


# log in
@app.route("/login_user")
def login_user(username, password):
    response = user_register.login_user(username, password)
    if response.status == 0:
        return {"access_token": response.token, "token_type": "bearer"}
    else:
        # if password cannot correctly be verified, we return -1 to indicate login has failed
        return -1


def signal_handler(sig, frame):
    """
    Handles the shut down of DS when ctrl-C is pressed.
    """
    print('DS Shutting down...')

    ds.shut_down()
    sys.exit(0)


if __name__ == "__main__":
    """
    Initializes an api to communicate by HTTP with a client.
    """
    parser = argparse.ArgumentParser(
        prog='DSClientAPI',
        description='A Client API for Data Station',
        epilog='Text at the bottom of help')
    parser.add_argument('-c', '--ds_config', default='data_station_config.yaml', type=str)
    parser.add_argument('-a', '--app_config', default='app_connector_config.yaml', type=str)
    parser.add_argument('-p', '--port', default=8080, type=int)
    parser.add_argument('-hs', '--host', default="localhost", type=str)

    signal.signal(signal.SIGINT, signal_handler)

    # TODO: remove these and put them somewhere else
    if os.path.exists("data_station.db"):
        os.remove("data_station.db")
    folders = ['SM_storage', 'Staging_storage']
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    args = parser.parse_args()
    print(args)
    port = args.port
    host = args.host

    global ds
    ds = initialize_system(args.ds_config, args.app_config)

    # TODO: remove these and put them somewhere else
    log_path = ds.data_station_log.log_path
    if os.path.exists(log_path):
        os.remove(log_path)

    app.run(debug=False, host=host, port=port)

    # signal.pause()
