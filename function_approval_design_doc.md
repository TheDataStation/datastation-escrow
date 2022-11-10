# Function Approval Design Doc

## Summary
A Docker container runs in Data Station that executes the arbitrary functions that users run on it. By only exposing one or two directories to the container, this keeps functions from accessing anything else on the system. A Docker image that simulates the environment run in Data Station is made publicly available for developers.

### Terms
- Owner: The owner of a dataset.
- User: The agent that runs functions within Data Station.
- Developer: The creator(s) of the functions to be run in Data Station by User(s).
- Docker Image vs. Container: An image is a read-only template for a Container, the actual environment that is run.

## Docker Image Specification
The Docker Image must be as close to what is actually run inside Data Station as possible. This image contains only two access points: 
1. A map to one directory that is protected and the only one that it is allowed to access

    Sample Data Station data directory: `/DS_storage/user_id/data`

    is mapped to the directory in Docker Container: `/data`
    
    Use https://askubuntu.com/questions/1372370/how-to-access-the-filesystem-inside-docker-containers-on-ubuntu-20-04 to map a directory directly to the filesystem.


2. A container network to receive the functions and communicate the outputs that the connector requests to run.
   
   This network is only between the container and the host machine.

### Data Station Dev Utils
These dev utils are included in the Docker image that we publish. They can probably just be Python functions for now.

Operations within the Docker container:

Operation | Inputs | Outputs | Description
-|-|-|-
Write | (2) params: filename, content | SUCCESS, etc. | Writes only to protected filesystem (only to `/data` in the container)
Read | (1) param: filename | Read file | Reads only to accessible data given by a policy

Operations outside of the Docker container:

Operation | Inputs | Outputs | Description
-|-|-|-
Start DS Docker Container | (4) params: image, host directory to map container's `/data`, connectors, functions | Network connection | Runs the Docker Container from the image selected, maps the given host directory to `/data`, uploads the functions to the containers, and begins the Docker network connection to communicate functions. Simulates what happens in actual DS.
Run Function Through Container | (2) params: uploaded function, function parameters, network connection | Function return value(s) | Runs the code the developer has written, in the way Data Station will call it. This is the key function run by the connector. This function connects through the container network to send the function to the container, which then calls the function requested.

### Docker Image
- Includes a script that sets up and listens on a network connection to host. When a function is sent, it executes the function. This should be run at startup.
- Packages / modules that are required to let the functions run.

## Process

### Development
1. The provided Docker image is downloaded through docker hub.
2. Functions are written to act on the data (using write / read functions provided).
3. Connectors are written so the functions can be called outside the container (using the "Run Function Through Container" function provided).
4. The developer runs the image through the "Start DS Docker Container" function and debugs accordingly.
5. Once finished, the developer uploads the [function, container] pair to Data Station.


### Run
1. The selected Docker image that the developer used is run by DS, creating a new container with the accessible directory mapped to `/data`.
2. The Docker container is added to a network to communicate between host and container.
3. The functions (and dependencies) needed by the developer are uploaded to the container through `docker cp`.
4. The container runs the connector, which runs function(s) through the "Run Through Container" that leverages the network in order to run the function in question.
5. The function value is returned to the user.