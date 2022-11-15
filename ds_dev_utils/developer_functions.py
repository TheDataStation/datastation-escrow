import docker
import time
import os
import tarfile


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

    os.chdir(os.path.dirname(src))
    srcname = os.path.basename(src)

    tar = tarfile.open(src + '.tar', mode='w')
    try:
        tar.add(srcname)
    finally:
        tar.close()

    data = open(src + '.tar', 'rb').read()
    container.put_archive(dst, data)

    # return docker.put_archive(src, dst)

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
        client = docker.from_env()

        # create image from dockerfile
        self.image, log = client.images.build(path="./docker/images", tag="ds_docker")
        print(self.image, log)

        # run a container with command. It's detached so it runs in the background.
        self.container = client.containers.run(self.image,
                                        detach=True,
                                        tty=True,
                                        volumes={data_dir: {
                                            'bind': '/mnt/data', 'mode': 'rw'}},
                                        remove=True,
                                        )

        # copy the function file into the container
        docker_cp(self.container,
                function_file,
                "/usr/src/ds/functions")

        # run setup script
        code, ret = self.container.exec_run("python setup.py")
        time.sleep(4)

        print(os.getcwd())

        # Docker prints logs as bytes
        print(ret.decode("utf-8"))
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

    def run():
        """
        """
        return

if __name__ == "__main__":
    session = ds_docker(
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/functions/example_one.py',
        "connector_file",
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/my_data',
        "image"
    )

    # use os.link()?
