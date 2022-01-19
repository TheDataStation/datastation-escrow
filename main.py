import os

def initialize_system():

    # setup dbservice (if needed)

    # setup gatekeeper (if needed)

    # setup client_api (if needed)

    return

def run_system():

    # start frontend
    os.system("uvicorn fast_api:app --reload")

    return


if __name__ == "__main__":
    print("Main")

    # parse input commands and config file
    initialize_system()
    run_system()
