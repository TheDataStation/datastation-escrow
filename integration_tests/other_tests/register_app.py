import sys
from common.utils import parse_config

from dsapplicationregistration.dsar_core import register_connectors, __test_registration

if __name__ == "__main__":

    # take path to connector from command line
    # FIXME: use argparse and do this properly
    app_config = parse_config(sys.argv[1])
    connector_name = app_config["connector_name"]
    connector_module_path = app_config["connector_module_path"]
    print("connector name: " + str(connector_name))
    print("connector module path: " + str(connector_module_path))

    # and register that module
    register_connectors(connector_name, connector_module_path)

    print("Testing registration")
    __test_registration()
