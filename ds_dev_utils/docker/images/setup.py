import time
import socket
import pickle
import os
import requests
from flask import Flask, request

from run_function import (load_connectors,
                            run_function)
from dsapplicationregistration.dsar_core import get_registered_functions

class function_server:
    """
    a simple socket-based server that listens for a connection to receive a
     function with pickled inputs, then turns off
    """

    def __init__(self, port_num):
        # initialize socket and bind to port and host
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket = s

        self.host = socket.gethostbyname("")
        # self.host = "host.docker.internal"
        self.port = port_num
        s.bind((self.host, self.port))

    def get_connection(self):
        # listen on socket
        s = self.socket
        s.listen()
        # conn, addr = s.accept()

        return s.accept()


app = Flask(__name__)
@app.route("/function", methods = ['POST', 'GET'])
def hello():
    if request.method == 'POST':
        # time.sleep(30)
        return "post Hello World!"
    else:
        return "Hello World!"

def main():
    # time.sleep(3)
    # load connectors before doing anything
    connector_dir = "/usr/src/ds/functions"
    load_connectors(connector_dir)
    print("setting up...")

    # notice that communication from container to host doesn't
    #  need a port mapping

    # send signal that the container has started, and keep trying until it succeeds
    #  stack overflow: https://stackoverflow.com/questions/27062099/python-requests-retrying-until-a-valid-response-is-received
    cnt = 0
    max_retry = 3
    while cnt < max_retry:
        try:
            response = requests.get('http://host.docker.internal:3030/started')
            if response.status_code == requests.codes.ok:
                break
            else:
                raise RuntimeError(
                        "requests.get: wrong status code")
        except requests.exceptions.RequestException as e:
            time.sleep(2**cnt)
            cnt += 1
            if cnt >= max_retry:
                raise e

    print(response.content)

    # request function to run
    response = requests.get('http://host.docker.internal:3030/function')
    function_dict = pickle.loads(response.content)
    print("function dictionary: ", function_dict)

    # run the function and pickle it
    ret = run_function(function_dict["function"], *function_dict["args"], **function_dict["kwargs"])
    to_send_back = pickle.dumps({"return_value":ret})

    print(to_send_back)

    # send the return value of the function
    response = requests.post("http://host.docker.internal:3030/function_return",
            data=to_send_back,
            # headers={'Content-Type': 'application/octet-stream'},
            )
    print(response, response.content)
    app.run(debug = False, host="0.0.0.0", port = 80)

    # # setup server and get connection
    # fs = function_server(2222)
    # print("set up function server")
    # conn, addr = fs.get_connection()

    # print("getting connection...")

    # # receive pickled function and input
    # with conn:
    #     print(f"Connected by {addr}")
    #     while True:
    #         data = conn.recv(1024)
    #         if not data:
    #             break

    #         # interpret pickled data and run
    #         inputs = pickle.loads(data)
    #         ret = run_function(inputs["function"], *inputs["args"], **inputs["kwargs"])
    #         # send result back
    #         to_send_back = pickle.dumps({"return_value":ret})
    #         # time.sleep(5)
    #         conn.sendall(to_send_back)

    #         # conn.sendall(bytes(str(ret), "utf-8"))

if __name__ ==  '__main__':
    main()