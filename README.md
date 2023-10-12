# Data Station

This project contains the source code for the Data Station, which is a computational and data management infrastructure 
designed to enable the formation of data-sharing consortia. (an intermediary **data escrow**)

## Setup

Run the following command from the root directory to install the necessary packages.

    pip install -r requirements.txt

Create the needed directories

    mkdir SM_storage SM_storage_mount

## Run a simple application

Here is the code to run a simple application: printing out the first line of a file.

Use the following configs in data_station_config.yaml

    epm_path: "example_epm/general_test.py"
    trust_mode: "full_trust"
    in_development: True

Execute the script that contains the example application.

    python3 -m integration_new.general_full_trust

## Enabling Docker to run functions

Ensure that you have Docker enabled on your machine.

Start Docker on macOS:

    open -a Docker

Start Docker on linux:

    sudo systemctl start docker
    sudo chmod 666 /var/run/docker.sock

Use the following configs in data_station_config.yaml

    in_development: False

## Notes For Developers
 
Data Station is a computational and data management infrastructure.
Developers will write applications they want to run on top of Data Station. 
In this section, we explain how to develop applications for Data Station.

An application that can run on Data Station is specified as a python file under the
example_epm/ directory, called an **EPM file**. An example of an EPM file is:

    example_epm/sharing_consortia.py

Each application (EPM file) exposes to users a set of APIs they can call. 
These are functions tagged with @api_endpoint. These APIs can include functionalities
like registering a data element, proposing a contract, etc. 
An example of an API endpoint in sharing_consortia.py is

    @api_endpoint
    def register_de(...):

A special class of APIs are additionally tagged with @function. 
These are APIs that users can call, that need to access the content of data elements. 
An example of such an API endpoint in sharing_consortia.py is

    @@api_endpoint
    @function
    def calc_pi_and_pip():

Data Station provides a set of default implementation for some of these 
functionalities. Those are written in escrowapi/escrow_api.py.

To run an application once it's written, modify data_station_config.yaml.

    epm_path: "example_epm/<Your EPM File>"

You can test the application by writing a script, or interact it through FastAPI 
interface. To use the FastAPI interface, run the following command:

    python3 -m server.fastapi_server

Then, in your browser, enter:

    http://localhost:8000/docs
