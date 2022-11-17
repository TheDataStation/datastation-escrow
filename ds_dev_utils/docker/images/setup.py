import time
import socket
import pickle
from run_function import (load_connectors,
                            run_function)


# for i in range(5):
#     print("hi")
#     time.sleep(1)

class function_server:
    def __init__(self, port_num):
        # initialize socket and bind to port and host
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket = s

        self.host = socket.gethostname()
        self.port = port_num
        s.bind((self.host, self.port))

    def get_data(self):
        # listen on socket
        s = self.socket
        s.listen()
        # conn, addr = s.accept()

        return s.accept()

        return


def main():
    # load connectors before doing anything
    connector_dir = "/usr/src/ds/functions"
    load_connectors(connector_dir)

    # setup server and get connection
    fs = function_server(2222)
    conn, addr = fs.get_data()

    # receive pickled function and input
    with conn:
        print(f"Connected by {addr}")
        while True:
            data = conn.recv(1024)
            if not data:
                break
            inputs = pickle.loads(data)
            ret = run_function(*inputs)
            conn.sendall(bytes(str(ret)))



if __name__ ==  '__main__':
    main()