import os
import json

from dsapplicationregistration.dsar_core import (get_functions_names,
                                                 get_registered_functions, )
from dbservice import database_api
from contractmanager import contract_manager
from common.abstraction import DataElement
from common.config import DSConfig

from verifiability.log import Log
from writeaheadlog.write_ahead_log import WAL
from crypto.key_manager import KeyManager
from ds_dev_utils.jail_utils import DSDocker, FlaskDockerServer


class Gatekeeper:
    def __init__(self,
                 data_station_log: Log,
                 write_ahead_log: WAL,
                 key_manager: KeyManager,
                 trust_mode: str,
                 epf_path,
                 config: DSConfig,
                 development_mode,
                 ):
        """
        The general class for the gatekeeper, which brokers access to data elements and
         runs jail functions
        """

        print("Start setting up the gatekeeper")
        self.config = config

        # save variables
        self.data_station_log = data_station_log
        self.write_ahead_log = write_ahead_log
        self.key_manager = key_manager
        self.trust_mode = trust_mode

        self.epf_path = epf_path
        self.mount_dir = self.config.ds_storage_path
        self.docker_id = 1
        self.server = FlaskDockerServer()
        self.server.start_server()

        # register all api_endpoints that are functions in database_api
        function_names = get_functions_names()
        # now we call dbservice to register these info in the DB
        for cur_f in function_names:
            database_service_response = database_api.create_function(cur_f)
            if database_service_response["status"] == 1:
                print("database_api.create_function: internal database error")
                raise RuntimeError(
                    "database_api.create_function: internal database error")

        f_res = database_api.get_all_functions()
        if f_res["status"] == 0:
            print("all function registered: ", f_res["data"])

        if not development_mode:
            docker_image_realpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".")
            print("docker image path: ", docker_image_realpath)
            self.docker_session = DSDocker(
                self.server,
                docker_image_realpath,
            )

        print("Gatekeeper setup success")

    def get_new_docker_id(self):
        ret = self.docker_id
        self.docker_id += 1
        return ret

    # We add times to the following function to record the overheads

    def call_api(self,
                 function,
                 caller_id,
                 start_de_id,
                 # contract_id,
                 *args,
                 **kwargs):
        """
        Calls the API specified, ensuring that
          - data is only exposed to the API if permitted
          - data accessed by API is allowed

        Parameters:
         function: api to call
         caller_id: caller id
         contract_id: id of contract from which the api is called,

        Returns:
         Response based on what happens
        """
        # Prepare information needed by Docker: all DEs, and the derived DE origins
        all_des = set()
        get_des_res = database_api.get_all_des()
        for cur_de in get_des_res["data"]:
            if self.trust_mode == "no_trust":
                data_owner_symmetric_key = self.key_manager.get_agent_symmetric_key(cur_de.owner_id)
            else:
                data_owner_symmetric_key = None
            cur_de = DataElement(cur_de.id,
                                 cur_de.store_type,
                                 cur_de.derived,
                                 data_owner_symmetric_key)
            all_des.add(cur_de)

        # Get all DE sets approved by policies (there can be multiple sets)
        param_json = {"args": args, "kwargs": kwargs}
        param_str = json.dumps(param_json)
        approve_de_sets = []

        approved_de_sets_res = database_api.get_approved_de_sets_from_policies(caller_id,
                                                                               function,
                                                                               param_str)
        # Each element of approved_de_sets_res is a str: we convert it to a set of DE IDs
        for cur_de_set in approved_de_sets_res:
            cur_de_set = set(map(int, cur_de_set[0].split(" ")))
            approve_de_sets.append(cur_de_set)

        # TODO: think about CMR's connection with contract approvals

        print(approve_de_sets)
        exit()

        # actual api call
        if self.trust_mode == "full_trust":
            agents_symmetric_key = None
        else:
            agents_symmetric_key = self.key_manager.agents_symmetric_key
        ret = call_actual_api(function,
                              self.epf_path,
                              self.config,
                              start_de_id,
                              agents_symmetric_key,
                              all_des,
                              self.get_new_docker_id(),
                              self.docker_session,
                              *args,
                              **kwargs,
                              )

        api_result = ret["return_info"][0]
        de_paths_accessed = ret["return_info"][1]
        derived_des_to_create = ret["return_info"][2]
        decryption_time = ret["return_info"][3]

        # print("Gakeeper printing function output", api_result)
        # print(de_paths_accessed)
        # print(derived_des_to_create)

        de_ids_accessed = []
        for path in de_paths_accessed:
            print(path)
            de_ids_accessed.append(int(path.split("/")[-2]))

        print("DE accessed is", de_ids_accessed)
        # print("accessible data by policy is", accessible_data_policy)
        # print("all accessible DE is", all_accessible_de_id)
        # print("Decryption time is", decryption_time)

        # We now check if it can be released by asking contract_manager.
        # We know the caller, the args, and DEs accessed (in development mode)
        release_status = contract_manager.check_release_status(caller_id,
                                                               set(de_ids_accessed),
                                                               function,
                                                               param_str)
        # print(release_status)
        if release_status:
            self.data_station_log.log_intent_policy_match(caller_id,
                                                          function,
                                                          de_ids_accessed,
                                                          self.key_manager)
            response = {"status": 0,
                        "message": "Contract result can be released",
                        "result": api_result,
                        "de_ids_accessed": de_ids_accessed,
                        "derived_des_to_create": derived_des_to_create}
        else:
            response = {"status": 1,
                        "message": "Result cannot be released",
                        "de_ids_accessed": de_ids_accessed,
                        "derived_des_to_create": derived_des_to_create}

        # if set(de_ids_accessed).issubset(set(all_accessible_de_id)):
        #     # print("All data access allowed by policy.")
        #     # log operation: logging intent_policy match
        #     self.data_station_log.log_intent_policy_match(cur_user_id,
        #                                                   function,
        #                                                   de_ids_accessed,
        #                                                   self.key_manager, )
        #     # In this case, we can return the result to caller.
        #     response = {"status": 0,
        #                 "message": "Contract result can be released",
        #                 "result": [api_result, decryption_time]}
        # elif set(data_ids_accessed).issubset(all_accessible_de_id):
        #     # print("Some access to optimistic data not allowed by policy.")
        #     # log operation: logging intent_policy mismatch
        #     self.data_station_log.log_intent_policy_mismatch(cur_user_id,
        #                                                      api,
        #                                                      data_ids_accessed,
        #                                                      set(accessible_de_policy),
        #                                                      self.key_manager, )
        #     response = APIExecResponse(status=-1,
        #                                message="Some access to optimistic data not allowed by policy.",
        #                                result=[api_result, data_ids_accessed], )
        # else:
        #     # log operation: logging intent_policy mismatch
        #     self.data_station_log.log_intent_policy_mismatch(cur_user_id,
        #                                                      function,
        #                                                      de_ids_accessed,
        #                                                      set(all_accessible_de_id),
        #                                                      self.key_manager, )
        #     response = {"status": 1,
        #                 "message": "Access to illegal DE happened. Something went wrong."}

        return response

    def shut_down(self):
        self.server.stop_server()


def call_actual_api(function_name,
                    epf_path,
                    config: DSConfig,
                    start_de_id,
                    agents_symmetric_key,
                    accessible_de,
                    docker_id,
                    docker_session: DSDocker,
                    *args,
                    **kwargs,
                    ):
    """
    The thread that runs the API on the Docker container

    Parameters:
     function_name: name of API to run on Docker container
     epf_path: path to the epf file
     config: DS config
     agents_symmetric_key: key manager storing all the sym keys
     accessible_de: a set of accessible DataElement
     docker_id: id assigned to docker container
     docker_session: docker container
     *args / *kwargs for api

    Returns:
     Result of api
    """

    print(os.path.dirname(os.path.realpath(__file__)))
    # print(api_name, *args, **kwargs)
    epf_realpath = os.path.dirname(os.path.realpath(__file__)) + "/../" + epf_path

    config_dict = {"accessible_de": accessible_de, "docker_id": docker_id, "agents_symmetric_key": agents_symmetric_key,
                   "operating_system": config.operating_system, "start_de_id": start_de_id}
    print("The real epf path is", epf_realpath)

    # print(session.container.top())

    # run function
    list_of_functions = get_registered_functions()

    # print(function_name)
    for cur_f in list_of_functions:
        if function_name == cur_f.__name__:
            # print(cur_f.__name__)
            print("call", function_name)
            docker_session.flask_run(function_name, epf_realpath, config.ds_storage_path, config_dict, *args, **kwargs)
            ret = docker_session.server.q.get(block=True)
            # print(ret)
            return ret

    # TODO clean up: uncomment line below in production
    # session.stop_and_prune()


# We add times to the following function to record the overheads

if __name__ == '__main__':
    print("Gatekeeper starting.")
