import os
import pickle
import sys
import argparse
from flask import Flask, request

from main import initialize_system

from userregister import user_register
from ds import DataStation

app = Flask(__name__)

# create user
@app.route("/call_api", methods=['post'])
def call_api():
    """
    Calls the API specified by a pickled dict-like structure. The dict must contain:
        username: the user that is calling this api
        api: the api to be called
        exec_mode: execution mode
        args: arguments for the api being called
        kwargs: arguments for the api being called
    """
    unpickled = (request.get_data())
    # print(unpickled)
    args = pickle.loads(unpickled)
    # print("Call API Arguments are:", args)
    # enqueue(api_call(data))

    return pickle.dumps(ds.call_api(args['username'], args['api'], args['exec_mode'], *args['args'], **args['kwargs']))

# create user
@app.route("/create_user", methods=['post'])
def create_user():
    """
    Creates a user from a pickled dict-like structure. The dict must contain:
        username: the user that is calling this api
        user_sym_key (optional): symmetric key for user
        user_public_key (optional): public key for user
    """

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog = 'DSClientAPI',
                    description = 'A Client API for Data Station',
                    epilog = 'Text at the bottom of help')
    parser.add_argument('-c','--ds_config', nargs='?', default='data_station_config.yaml', type=str)
    parser.add_argument('-a','--app_config', nargs='?', default='app_connector_config.yaml', type=str)
    parser.add_argument('-p','--port', nargs='?', const=8080, type=int)

    args = parser.parse_args()
    print(args)
    port = args.port

    global ds
    # ds = initialize_system(args.ds_config, args.app_config)
    ds = initialize_system(args.ds_config, args.app_config)

    app.run(debug=False, host="localhost", port=port)
