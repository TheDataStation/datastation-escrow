import time
import socket
import pickle
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

        self.host = socket.gethostname()
        self.port = port_num
        s.bind((self.host, self.port))

    def get_connection(self):
        # listen on socket
        s = self.socket
        s.listen()
        # conn, addr = s.accept()

        return s.accept()


def main():
    # load connectors before doing anything
    connector_dir = "/usr/src/ds/functions"
    load_connectors(connector_dir)

    # setup server and get connection
    fs = function_server(2222)
    conn, addr = fs.get_connection()

    # receive pickled function and input
    with conn:
        print(f"Connected by {addr}")
        while True:
            data = conn.recv(1024)
            if not data:
                break

            # interpret pickled data and run
            inputs = pickle.loads(data)
            ret = run_function(inputs["function"], *inputs["args"], **inputs["kwargs"])
            # send result back
            conn.sendall(bytes(str(ret), "utf-8"))

if __name__ ==  '__main__':
    main()