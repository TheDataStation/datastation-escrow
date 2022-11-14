import docker
import time

client = docker.from_env()

# create image from dockerfile
image, log = client.images.build(path="./docker/images", tag="ds_docker")
print(image,log)

# run a container with command. It's detached
container = client.containers.run(image, 
                                  "python setup.py",
                                  detach=True,
                                  tty=True,
                                  volumes={'/Users/christopherzhu/Documents/chidata/DataStation/ds_dev_utils/my_data': {'bind':'/mnt/data', 'mode':'rw'}},
                                  remove=True,
                                  )

time.sleep(3)

# Docker prints logs as bytes
print(container.logs().decode("utf-8"))

running_containers = client.containers.list()

# container.stop()
print(running_containers)