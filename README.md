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

    epf_path: "example_epf/general_test.py"
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
