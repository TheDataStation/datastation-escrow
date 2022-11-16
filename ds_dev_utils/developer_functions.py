import docker
import time
import os
import tarfile
import pickle


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
    dir_path = os.path.dirname(os.path.realpath(__file__))

    # change directory to create tar file
    os.chdir(os.path.dirname(src))
    srcname = os.path.basename(src)

    tar = tarfile.open(src + '.tar', mode='w')
    try:
        tar.add(srcname)
    finally:
        tar.close()

    data = open(src + '.tar', 'rb').read()
    container.put_archive(dst, data)

    os.chdir(dir_path)

class ds_docker:
    def __init__(self,function_file, connector_file, data_dir, image):
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
        self.image, log = self.client.images.build(path="./docker/images", tag="ds_docker")
        print(self.image, log)

        # run a container with command. It's detached so it runs in the background.
        self.container = self.client.containers.create(self.image,
                                        detach=True,
                                        tty=True,
                                        volumes={data_dir: {
                                            'bind': '/mnt/data', 'mode': 'rw'}},
                                        )

        # copy the function file into the container
        docker_cp(self.container,
                function_file,
                "/usr/src/ds/functions")

        self.container.start()
        # run setup script
        code, ret = self.container.exec_run("python setup.py")

        # Docker prints logs as bytes
        print("Setup Output: \n" + ret.decode("utf-8"))
        # print(container.logs().decode("utf-8"))

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

    def connector_run(self):
        """

        """
        return

    def direct_run(self, function_name, *args, **kwargs):

        # create pickle file and dump
        filename = self.base_dir + "/functions/pickled_"+function_name
        arg_dict = {"args":args,"kwargs":kwargs}
        with open(filename,'wb') as pf:
            pickle.dump(arg_dict,pf)

        # send pickle file to container
        docker_cp(self.container,
            filename,
            "/usr/src/ds/pickled")

        # run the function
        code, ret = self.container.exec_run("python run_function.py " + function_name)
        print("direct run output: \n" + ret.decode("utf-8"))


if __name__ == "__main__":
    session = ds_docker(
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/functions/example_one.py',
        "connector_file",
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/my_data',
        "image"
    )
    session.direct_run("read_file", "/mnt/data/hi.txt")
    session.stop_and_prune()
    # use os.link()?
