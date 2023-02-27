import docker
import time
import os
import tarfile
import pickle
import socket
import requests
from multiprocessing import Process, Event, Queue, Manager
from flask import Flask, request

class FlaskDockerServer:
    def __init__(self, host="localhost", port=3030):
        """
        Initializes a Flask server to serve all docker containers.
         DOES NOT start server; call start_server() for that.

        Parameters:
         port: port to start server on, default 3030
         host: host to start server on, default localhost

        Returns:
         Nothing
        """
        self.port = port
        self.host = host
        self.manager = Manager()
        self.q = Queue()
        self.function_dict_to_send = self.manager.dict()

    def start_server(self):
        """
        STARTS the flask server in a new thread.

        Parameters:
         Nothing

        Returns:
         Nothing
        """
        self.server = Process(target=flask_thread, args=(self.port, self.q, self.function_dict_to_send))
        self.server.start()
        return

    def stop_server(self):
        """
        Stops the flask server by terminating the thread it is running on.

        Parameters:
         Nothing

        Returns:
         Nothing
        """
        self.server.terminate()
        self.server.join()

def docker_cp(container, src, dst):
    """
    Code modified from:
    https://stackoverflow.com/questions/46390309/how-to-copy-a-file-from-host-to-container-using-docker-py-docker-sdk

    Parameters:
     container: container to send file to
     src: the localhosts's source file (FULL path)
     dst: the container's filesystem

    Returns:
     The error (if any) of the function put_archive
    """
    # save working directory
    dir_path = os.path.abspath(os.curdir)

    # change directory to create tar file. This is necessary since
    # the tar file is relative
    os.chdir(os.path.dirname(src))
    srcname = os.path.basename(src)

    tar = tarfile.open(src + '.tar', mode='w')
    try:
        tar.add(srcname)
    finally:
        tar.close()

    data = open(src + '.tar', 'rb').read()
    container.put_archive(dst, data)

    # go back to working directory
    os.chdir(dir_path)


class DSDocker:
    # HOST = "127.0.0.1"
    HOST = socket.gethostbyname("")  # The server's hostname or IP address
    PORT = 3000  # The port used by the server

    def __init__(self, server: FlaskDockerServer, function_file, data_dir, config_dict, dockerfile):
        """
        Initializes a docker container with mount point data_dir, with
        image given. Loads function file into container.
        Image currently does nothing.

        Parameters:
        function_file: the file containing the functions to be run
        connector_file: the file containing the connectors that call
        functions within the function_file
        data_dir: directory to mount data to container
        image: docker image (from hub) to build off on

        Returns:
        """

        print("config_dict contents: ", config_dict)

        # In here we convert the accessible_data_dict to paths that Docker sees
        self.docker_id = config_dict["docker_id"]
        self.server = server

        # cur_dir = os.path.dirname(os.path.realpath(__file__))
        # accessible_path = os.path.join(cur_dir, "docker/images/accessible.pkl")

        self.base_dir = os.path.dirname(os.path.realpath(__file__))
        self.client = docker.from_env()

        # create image from dockerfile
        self.image, log = self.client.images.build(
            path=dockerfile, tag="ds_docker")
        # print(self.image, log)

        print("Created image!")

        # run a container with command. It's detached so it runs in the background
        #  It is loaded with a setup script that starts the server.

        # tty allows commands such as "docker cp" to be run
        # ports define ports. These are arbitrary
        self.container = self.client.containers.create(self.image,
                                                       "python setup.py",
                                                       detach=True,
                                                       tty=True,
                                                    #    ports={
                                                    #        '80/tcp': self.PORT,
                                                    #    },
                                                       cap_add=["SYS_ADMIN", "MKNOD"],
                                                       devices=["/dev/fuse:/dev/fuse:rwm"],
                                                       volumes={data_dir: {
                                                           'bind': '/mnt/data', 'mode': 'rw'}},
                                                       )

        cur_dir = os.path.dirname(os.path.realpath(__file__))
        config_dict_file = os.path.join(cur_dir, "args.pkl")

        config_dict_file_pkl = pickle.dumps(config_dict)
        f = open(config_dict_file, "wb")
        f.write(config_dict_file_pkl)
        f.close()

        docker_cp(self.container,
                  config_dict_file,
                  "/usr/src/ds")

        # copy the function file into the container
        docker_cp(self.container,
                  function_file,
                  "/usr/src/ds/functions")

    def stop_and_prune(self):
        """
        Stops the given Docker container instance and removes any non-running containers

        Parameters:
        client: the docker client to prune
        container: container to stop

        Returns:
        Nothing
        """
        self.container.stop()
        self.client.containers.prune()

    def flask_run(self, function_name, *args, **kwargs):
        """
        utilizes a flask server to send functions

        Parameters:
         function_name: name of function to run
         args, kwargs: arguments for the function

        Returns:
         function return value
        """

        # create a dictionary to pickle
        func_dict = {"function": function_name, "args": args, "kwargs": kwargs}

        self.server.function_dict_to_send[self.docker_id] = func_dict

        # time.sleep(1)
        self.container.start()


def flask_thread(port, q: Queue, function_dict_to_send):
    """
    the thread function that gets run whenever DSDocker.flask_run is called

    Parameters:
     shutdown_event: setting this event causes the main thread to kill this thread
     q: queue to share data
     function_dict_to_send: a shared function dict that sends based on container ID

    Returns:
     Nothing
    """

    app = Flask(__name__)

    @app.route("/started")
    def started():
        id = int(request.args.get('docker_id'))
        print("received from: ", id)
        return "Start received!"

    @app.route("/get_function_dict")
    def function():
        docker_id = int(request.args.get('docker_id'))
        print("sending function from docker_id: ", docker_id)
        return pickle.dumps(function_dict_to_send[docker_id])

    @app.route("/get_function_return", methods=['post'])
    def function_return():
        docker_id = int(request.args.get('docker_id'))

        print("Starting function return from docker_id: ", docker_id)
        # unpickle request data
        unpickled = (request.get_data())
        print(unpickled)
        ret_dict = pickle.loads(unpickled)
        print("Returned dictionary is", ret_dict)
        ret = (ret_dict["return_value"], ret_dict["data_accessed"])
        print("Child Thread, return value: ", ret)

        # add to shared queue
        q.put({"docker_id": docker_id, "return_value": ret})
        return "Received return value from container."

    # run the flask app
    print("Child thread: flask app starting...")
    app.run(debug=False, host="localhost", port=port)


if __name__ == "__main__":
    # app.run(debug = True, port = 3000)
    server = FlaskDockerServer()

    config_dict = {'docker_id':1,'accessible_data_dict':{'/mnt/data/hi.txt'}}

    # create a new ds_docker instance
    session = DSDocker(
        server,
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/example_functions/example_one.py',
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/example_data',
        config_dict,
        "./docker/images"
    )

    # run function
    session.flask_run("line_count")
    session.flask_run("line_count")

    for i in range(2):
        ret = server.q.get(block=True)
        print(ret)

    # clean up
    session.stop_and_prune()
