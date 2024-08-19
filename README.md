# Programmable Dataflows

Data sharing is central to a wide variety of applications such as
fraud detection, ad matching, and research. Yet, the lack of data sharing abstractions makes the solution to each data sharing problem
bespoke and cost-intensive, hampering value generation.This project contains the source code for **programmable dataflows**, a programming model 
for implementing any data sharing problems with a new **contract** abstraction, allowing people to move towards a 
common sharing goal without violating any regulatory, privacy, or preference constraints.
The programming model is implemented on top of an intermediary **data escrow**. ([Link to programmable dataflow paper](https://arxiv.org/pdf/2408.04092)) ([Link to data escrow paper](https://arxiv.org/pdf/2305.03842))

## What are data sharing problems?

We refer to any scenario in which one party wants access to anothers data a *data sharing problem*.
Examples include: advertisers use a data cleanroom to run analysis on joint data; national patient
registry shares medical data with researchers for causal discovery; banks pool data to train joint
fraud detection model.

## What makes data sharing problems challenging?

Consider the following data sharing problem: a few banks are interested
in pooling credit card transaction data to train more accurate fraud
detection models, subject to the following constraints. 1) It is not commercially interesting to pool their data, unless every bank has the guarantee that the joint
model benefits themselves, instead of only helping others. 2) The shared data should only
be used for model training, and nothing else.

A main challenge of data sharing is that people lack information
to assess whether a dataflow is desirable before it takes place. For example, the
banks only want to release a joint model if it meets some accuracy threshold,
but they have no way of ensuring that without sharing the data to train the model. Additionally,
banks have no guarantee that the other banks will only use the data for model training, once they share
the raw data. To preclude adverse consequences, many people default to not sharing.

## Solving the challenges with programmable dataflows

We introduce a new *contract* abstraction that bounds the consequences of each dataflow by making it explicit *who* contributes data, *what
computation* takes place on that data, *who* receives the result,
and under *what conditions*. Importantly, it provides this information before an intended dataflow takes place, thus addressing
the challenge by helping agents make an informed decision on
whether to allow the dataflow. The programming model implements the contract abstraction, enabling people to solve
any data sharing problem through a sequence of contract *propositions*, *approvals*, and *executions*.

# Set up the repo

Clone the repo.

    git clone https://github.com/TheDataStation/DataStation.git

Run the following command from the root directory to install the necessary packages.

    pip install -r requirements.txt

Create the needed directories

    mkdir SM_storage SM_storage_mount

## Run a simple application

Here is the code to run a simple data sharing application: share the schema of a csv file with others.

Use the following configs in data_station_config.yaml

    cpm_path: "example_cpm/share_schema_app.py"
    trust_mode: "full_trust"
    in_development: True

Execute the script that contains the example application.

    python3 -m integration_new.general_full_trust

Alternatively, to access the application through a web UI (FastAPI):

    python3 -m server.fastapi_server

## Enabling Docker to run functions

Ensure that you have Docker enabled on your machine.

Start Docker on macOS:

    open -a Docker

Start Docker on linux:

    sudo systemctl start docker
    sudo chmod 666 /var/run/docker.sock

Use the following configs in data_station_config.yaml

    in_development: False

[//]: # (## Notes For Developers)

[//]: # ( )
[//]: # (Data Station is a computational and data management infrastructure.)

[//]: # (Developers will write applications they want to run on top of Data Station. )

[//]: # (In this section, we explain how to develop applications for Data Station.)

[//]: # ()
[//]: # (An application that can run on Data Station is specified as a python file under the)

[//]: # (example_epm/ directory, called an **EPM file**. An example of an EPM file is:)

[//]: # ()
[//]: # (    example_epm/sharing_consortia.py)

[//]: # ()
[//]: # (Each application &#40;EPM file&#41; exposes to users a set of APIs they can call. )

[//]: # (These are functions tagged with @api_endpoint. These APIs can include functionalities)

[//]: # (like registering a data element, proposing a contract, etc. )

[//]: # (An example of an API endpoint in sharing_consortia.py is)

[//]: # ()
[//]: # (    @api_endpoint)

[//]: # (    def register_de&#40;...&#41;:)

[//]: # ()
[//]: # (A special class of APIs are additionally tagged with @function. )

[//]: # (These are APIs that users can call, that need to access the content of data elements. )

[//]: # (An example of such an API endpoint in sharing_consortia.py is)

[//]: # ()
[//]: # (    @api_endpoint)

[//]: # (    @function)

[//]: # (    def calc_pi_and_pip&#40;&#41;:)

[//]: # ()
[//]: # (Data Station provides a set of default implementation for some of these )

[//]: # (functionalities. Those are written in escrowapi/escrow_api.py.)

[//]: # ()
[//]: # (To run an application once it's written, modify data_station_config.yaml.)

[//]: # ()
[//]: # (    epm_path: "example_epm/<Your EPM File>")

[//]: # ()
[//]: # (You can test the application by writing a script, or interact it through FastAPI )

[//]: # (interface. To use the FastAPI interface, run the following command:)

[//]: # ()
[//]: # (    python3 -m server.fastapi_server)

[//]: # ()
[//]: # (Then, in your browser, enter:)

[//]: # ()
[//]: # (    http://localhost:8000/docs)
