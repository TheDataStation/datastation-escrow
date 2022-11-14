import docker

client = docker.from_env()

image, log = client.images.build(path="./docker/images", tag="ds_docker")
print(log)
print(client.containers.run(image, "python setup.py"))