# Function Approval Design Doc

## Summary
A Docker container runs in Data Station that executes the arbitrary functions that users run on it. By only exposing one or two directories to the container, this keeps functions from accessing anything else on the system. A Docker image that simulates the environment run in Data Station is made publicly available for developers.

### Terms
- Owner: The owner of a dataset.
- User: The agent that runs functions within Data Station.
- Developer: The creator(s) of the functions to be run in Data Station by User(s).

## Docker Image Specification
The Docker Image must be as close to what is actually run inside Data Station as possible. This image contains a connection to one filesystem: the protected one that it is allowed to access.

Data Station Filepath: `/DS_storage/user_id/data`

is mapped to Docker Container: `/data`

Use https://askubuntu.com/questions/1372370/how-to-access-the-filesystem-inside-docker-containers-on-ubuntu-20-04 to map a directory directly to the filesystem.

### Data Station Dev Utils
These dev utils are included in the Docker image that we publish. They can probably just be Python functions for now.

Operation | Description
-|-
Write | Writes only to protected filesystem
Read | Reads only to accessible data given by a policy
Run Through | Runs the code the developer has written, in the way Data Station will call it. This is run by the connector. This function runs `docker exec` on a script that comes with the image that calls the function in question.

### Docker Image
- Includes a script that sets up a network connection to host.

## Process

### Development
1. The provided Docker image is downloaded through docker hub.
2. The developer runs the image and maps `/data` to a local directory for trial runs.
3. Functions are written to act on the data.
4. Connectors are written so the functions can be called outside the container.
5. 

### Run
1. The selected Docker image that the developer used is run by DS, creating a new container.
2. The Docker container is added to a network to communicate between host and container.
3. The functions (and dependencies) needed by the developer are uploaded to the container through `docker cp`.
4. The container runs the function(s) through the "Run Through Container" that leverages the network in order to run the function in question.
5. 