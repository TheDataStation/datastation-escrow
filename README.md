# Data Station

This project contains the source code for the Data Station, which is a computational and data management infrastructure 
designed to enable the formation of data-sharing consortia. (an intermediary **data escrow**)

## Setup

Run the following command from the root directory to install the necessary packages.

    pip install -r requirements.txt

Create the needed directories

    mkdir SM_storage SM_storage_mount

## Run an example application

We run a simple machine file sharing application (logistic regression), under the near zero trust mode.

Set the trust mode setting to "no_trust" in data_station_config.yaml

    trust_mode: "no_trust"

Set the content of app_connector_config.yaml as follows:

    connector_name: "file_sharing10"
    connector_module_path: "app_connectors/file_sharing10.py"

Create the needed directories

    mkdir test/test_file_sharing

Execute the script that contains the example application.

    python -m test.file_sharing_test workload_config.yaml

## Test a new application

Coming up soon.