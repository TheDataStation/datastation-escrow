import docker
import time
import os
import tarfile
import pickle
import socket


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


class ds_docker:
    HOST = socket.gethostname()  # The server's hostname or IP address
    PORT = 12345  # The port used by the server

    def __init__(self, function_file, connector_file, data_dir, dockerfile):
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

        # run a container with command. It's detached so it runs in the background
        #  It is loaded with a setup script that starts the server.
        self.container = self.client.containers.create(self.image,
                                                       "python setup.py",
                                                       detach=True,
                                                       #  tty allows commands such as "docker cp" to be run
                                                       tty=True,
                                                       # define ports. These are arbitrary
                                                       ports={
                                                           '2222/tcp': self.PORT},
                                                       # mount volumes
                                                       volumes={data_dir: {
                                                           'bind': '/mnt/data', 'mode': 'rw'}},
                                                       )

        # copy the function file into the container
        docker_cp(self.container,
                  function_file,
                  "/usr/src/ds/functions")

        # start the container
        self.container.start()

        # create container network
        self.network = self.client.networks.create("jail_network",
                                                   driver="bridge",
                                                   # shut off internet access
                                                   internal=True,
                                                   check_duplicate=True,
                                                   )

        # connect container to network
        self.network.connect(self.container)

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

    def network_remove(self):
        """
        disconnects container from network and removes the network
        """
        self.network.disconnect(self.container)
        self.network.remove()

    def connector_run(self, connector_name):
        """
        TODO: implement
        calls a developer-written connector, which then calls the function within the
         docker container

        Parameters:
         connector_name: name of connector to run

        Returns:
         connector return value
        """
        return

    def direct_run(self, function_name, *args, **kwargs):
        """
        OLD: communicates with docker to container to run a function, only by
         using the Python SDK (docker_cp, etc.) and pickle

        Parameters:
         function_name: name of function to run
         args, kwargs: arguments for the function

        Returns:
         function return value
        """

        # create pickle file and dump
        filename = self.base_dir + "/functions/pickled_"+function_name
        arg_dict = {"args": args, "kwargs": kwargs}
        with open(filename, 'wb') as pf:
            pickle.dump(arg_dict, pf)

        # send pickle file to container
        docker_cp(self.container,
                  filename,
                  "/usr/src/ds/pickled")

        # run the function
        code, ret = self.container.exec_run(
            "python run_function.py " + function_name)
        print("Direct run output: \n" + ret.decode("utf-8"))
        return ret

    def network_run(self, function_name, *args, **kwargs):
        """
        utilizes a docker network to send functions through sockets

        Parameters:
         function_name: name of function to run
         args, kwargs: arguments for the function

        Returns:
         function return value
        """

        # create a dictionary to pickle
        func_dict = {"function": function_name, "args": args, "kwargs": kwargs}

        # make pickle from dictionary
        to_send = pickle.dumps(func_dict)

        # connect to socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.HOST, self.PORT))

            # send the pickled function to docker container
            s.sendall(to_send)

            # receive output
            data = s.recv(1024)

        print(f"Network run output: {data.decode('utf-8')}")
        return data


if __name__ == "__main__":
    # create a new ds_docker instance
    session = ds_docker(
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/example_functions/example_one.py',
        "connector_file",
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/example_data',
        "./docker/images"
    )

    print(session.container.top())

    # run function
    # session.direct_run("read_file", "/mnt/data/hi.txt")
    session.network_run("line_count")

    # clean up
    session.network_remove()
    session.stop_and_prune()
