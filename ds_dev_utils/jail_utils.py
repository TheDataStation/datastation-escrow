import docker
import time
import os
import tarfile
import pickle
import socket
import requests
from multiprocessing import Process, Event, Queue
from flask import Flask, request


def docker_cp(container, src, dst):
    """
    Code modified from:
    https://stackoverflow.com/questions/46390309/how-to-copy-a-file-from-host-to-container-using-docker-py-docker-sdk

    Parameters:
     container: container to send file to
     src: the localhosts's source file
     dst: the container's filesystem

    Returns:
     The error (if any) of the function put_archive
    """
    # save working directory
    dir_path = os.path.dirname(os.path.realpath(__file__))

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

    def __init__(self, function_file, data_dir, dockerfile):
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
                                                       ports={
                                                           '80/tcp': self.PORT,
                                                       },
                                                       cap_add=["SYS_ADMIN", "MKNOD"],
                                                       devices=["/dev/fuse:/dev/1:rwm"],
                                                       volumes={data_dir: {
                                                           'bind': '/mnt/data', 'mode': 'rw'}},
                                                       )

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

        # Create a shutdown event and queue to share data between threads
        shutdown_event = Event()
        q = Queue()

        # create a dictionary to pickle
        func_dict = {"function": function_name, "args": args, "kwargs": kwargs}
        # make pickle from dictionary
        to_send = pickle.dumps(func_dict)

        # create a new thread for the flask server
        server = Process(target=flask_thread, args=(shutdown_event, q, to_send,))
        server.start()

        # time.sleep(1)
        self.container.start()

        # wait to shut down, then get the return value inside the queue
        shutdown_event.wait()
        print("Event done waiting!")
        return_value = q.get()
        print("Main thread: ", return_value)

        # end the process and join threads
        server.terminate()
        server.join()

        return return_value

def flask_thread(shutdown_event:Event, q:Queue, to_send):
    """
    the thread function that gets run whenever DSDocker.flask_run is called

    Parameters:
     shutdown_event: setting this event causes the main thread to kill this thread
     q: queue to share data
     to_send: the function to send to the docker container

    Returns:
     Nothing
    """

    app = Flask(__name__)
    @app.route("/started")
    def started():
        print("received")
        return "Start received!"

    @app.route("/function")
    def function():
        print("function")
        return to_send

    @app.route("/function_return", methods=['post'])
    def function_return():
        # unpickle request data
        unpickled = (request.get_data())
        ret_dict = pickle.loads(unpickled)
        ret = ret_dict["return_value"]
        print("Child Thread, return value: ", ret)

        # add to shared queue
        q.put(ret)

        # signal shutdown
        shutdown_event.set()
        return 'Request received. Server shutting down...'

    # run the flask app
    print("Child thread: flask app starting...")
    app.run(debug = False, host="localhost", port=3030)


if __name__ == "__main__":
    # app.run(debug = True, port = 3000)

    # create a new ds_docker instance
    session = DSDocker(
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/example_functions/example_one.py',
        "connector_file",
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/example_data',
        "./docker/images"
    )

    # run function
    session.flask_run("line_count")

    # clean up
    session.stop_and_prune()
