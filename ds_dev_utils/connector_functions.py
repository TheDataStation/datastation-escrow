import docker
from jail_utils import DSDocker

def docker_run(docker_connection, function_name):

    return

if __name__ == "__main__":
    session = DSDocker(
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/functions/example_one.py',
        "connector_file",
        '/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/my_data',
        "image"
    )
    session.stop_and_prune()
