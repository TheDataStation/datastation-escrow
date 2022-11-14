# Function Approval Design Doc

## Summary

### Introduction
A Docker container runs in Data Station that executes the arbitrary functions that users run on it. By only exposing one or two directories and a Docker network to the container, this keeps functions from accessing anything else on the system. A Docker image and dev utils are made publicly available for developers; together these two simulate the environment run in Data Station.

### Terms
- Owner: The owner of a dataset, who creates policies and uploads datasets.
- User: The agent that runs functions within Data Station.
- Developer: The creator(s) of the functions to be run in Data Station by User(s).
- Docker Image vs. Container: An image is a read-only template for a Container, the actual environment that is run in.
- Container Network: How Docker containers communicate with each other. Can also communicate with localhost.

### Why Dockerize?
Having a Docker container run functions within Data Station bounds functions within the Docker container environment, meaning the function cannot access anything outside of it.

Another effect of Dockerizing the environment is it allows for better function development and a data cleaning scheme:
- Through a Docker image that replicates Data Station behavior, it helps the developer write functions on an environment that is similar to the actual DS environment.
- With a Docker image, a data owner can run functions locally on a similar environment as well. By letting developers send owners functions and connectors, it facilitates the process of putting the onus on the data owner to tailor their data to the function.

## Docker Image Specification

A container running on Data Station contains **only two ways to access it**: 
1. A map between a localhost directory to a container directory through Docker; it is the only localhost directory the container is allowed to access.

    The Data Station data directory for the current user: `/DS_storage/user_id/data`

    is mapped to the directory in Docker Container: `/data`

    Use https://askubuntu.com/questions/1372370/how-to-access-the-filesystem-inside-docker-containers-on-ubuntu-20-04 to map a directory directly to the filesystem.

    Thus read/write on a Data Station directory are only granted to a container through `/data`.

2. A Docker network to send the functions to the container and communicate the outputs from the container to Data Station.
   
   This network has only two hosts: the container and the rest of Data Station (localhost).

### Data Station Dev Utils
These dev utils are included in the Docker image that we publish. They can probably just be Python functions for now.

Operations to be called **within** the Docker container:

Operation | Inputs | Outputs | Description
-|-|-|-
Write | (2) params: filename, content | SUCCESS, etc. | Writes only to protected filesystem (only to `/data` in the container)
Read | (1) param: filename | Read file | Reads only to accessible data given by a policy

Operations outside of the Docker container:

Operation | Inputs | Outputs | Description
-|-|-|-
Start DS Docker Container (`ds_docker_init`) | (4) params: DS-compatible image, localhost directory mapping to `/data`, connectors, functions | Network connection | For Developers and Owners. Simulates exactly what happens in DS. <ul><li>Runs the Docker Container from the image selected</li><li>Maps the given host directory to `/data`.</li><li>Uploads the functions to the containers</li><li>Begins the Docker network connection to send functions/receive return values</li></ul>
Run Function Through Container (`ds_docker_run`) | (3) params: uploaded function, function parameters, network connection | Function return value(s) | Runs the function the developer has written, in the way Data Station will call it (through the Docker network). This is a **key building block** for the connector, since it is the only interface to calling functions that are within the container.

### Docker Image
The Docker Image published to owners and developers is exactly the same as the image actually run inside Data Station. It includes:
- A script that sets up and **listens on the Docker network**. When a function is sent over the network from DS to the container, the container executes the function. This should be run at container startup.
- `/data` directory
- Packages / modules that are required to let the functions run.

## Process

### Process for Developers
These steps all occur on a local machine (**not** Data Station).
1. The provided Docker image is downloaded through docker hub.
2. Functions are written to act on the data (using write / read functions provided).
3. Connectors are written so the functions can be called outside the container.
4. The developer runs the image through the `ds_docker_init` function and runs the connectors that then run functions. They debug accordingly.
5. Once finished, the developer uploads the [function, container] pair to Data Station.


### Process for Data Owners
These steps all occur on a local machine (**not** Data Station).
1. The Docker image specified by the developer is downloaded through docker hub.
2. The functions written by the developer are downloaded.
3. The connectors written by the developer are downloaded.
4. The Data Owner runs the image through the `ds_docker_init` function and runs the connectors that then run functions.
5. If the results are satisfactory:
   1. The Data Owner uploads their transformed data to Data Station
   2. The Data Owner creates a policy for the data and function.


### What Happens on Data Station (when a user requests computation)
The `ds_docker_init` function should follow steps [1-4] as closely as possible.

1. The selected Docker image that the developer used is run by DS, creating a new container with the directory `DS_storage/user_id/data` mapped to `/data`.
2. The Docker container is added to a network to communicate between host and container.
3. At the same time, the Docker container runs the script given with the image to connect listen on the network.
4. The functions (and dependencies) needed by the developer are uploaded to the container through `docker cp`.
5. The Gatekeeper runs the connector, which runs function(s) through the "Run Through Container" that goes through the network in order to run the function in question.
6. The function value is returned to the Gatekeeper.

### Comments on the Process
- Notice that the Docker image is always exactly the same. It is provided by the Data Station writers.
- Only the **functions and connectors** are exchanged.
- There is very little difference in how the data owner and the developer use the Docker image locally.
- Functions and connectors aren't scanned for malicious code; the Data Owner is at risk when downloading and running them.