# Data Station

This project contains the source code for the Data Station, which is a computational and data management infrastructure 
designed to enable the formation of data-sharing consortia. (an intermediary **data escrow**)

## Setup

Run the following command from the root directory to install the necessary packages.

    pip install -r requirements.txt

Create the needed directories

    mkdir SM_storage SM_storage_mount

## Run an example application

We run a simple file sharing application, in which one user allows another user to see the number of lines 
in the shared files (but not the file itself).

Set the trust mode setting to "no_trust" in data_station_config.yaml

    trust_mode: "full_trust"

Set the content of app_connector_config.yaml as follows:

    connector_name: "example_one"
    connector_module_path: "app_connectors/example_one.py"

Execute the script that contains the example application.

    python -m test.example_one

## Explanation of example one

Please refer to the code in test/example_one.

In step 0, Data Station is set up. In step 1, two users are created, one being the data provider
and one being the data user. In step 2, data provider logs in and uploads three datasets 
to the Data Station. In step 3, data provider shares the first and the third dataset 
with the data user by writing policies. (but not the second). In step 4, data user logs in 
and checks all the available APIs. In the last step, data user calls the API "line_count", 
which prints the number of lines in all the files that he has access to. In this case,
it is the two files that the data provider shared with him.

## Test a new application

Coming up soon.