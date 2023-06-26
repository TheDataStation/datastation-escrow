import os
import pathlib
from dsapplicationregistration.dsar_core import (get_api_endpoint_names,
                                                 get_functions_names,
                                                 get_registered_functions, )
from dbservice import database_api
from sharemanager import share_manager
from policybroker import policy_broker
from common.pydantic_models.api import API
from common.pydantic_models.response import Response, APIExecResponse
from common.abstraction import DataElement

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
                 mount_dir,
                 development_mode,
                 ):
        """
        The general class for the gatekeeper, which brokers access to data elements and
         runs jail functions
        """

        print("Start setting up the gatekeeper")

        # save variables
        self.data_station_log = data_station_log
        self.write_ahead_log = write_ahead_log
        self.key_manager = key_manager
        self.trust_mode = trust_mode

        self.epf_path = epf_path
        self.mount_dir = mount_dir
        self.docker_id = 1
        self.server = FlaskDockerServer()
        self.server.start_server()

        # register all api_endpoints that are functions in database_api
        function_names = get_functions_names()
        # now we call dbservice to register these info in the DB
        for cur_api in function_names:
            api_db = API(api_name=cur_api)
            print("api added: ", api_db)
            database_service_response = database_api.create_api(api_db)
            if database_service_response.status == -1:
                print("database_api.create_api: internal database error")
                raise RuntimeError(
                    "database_api.create_api: internal database error")

        api_res = database_api.get_all_apis()
        print("all apis uploaded, with pid: ", os.getpid(), api_res)

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

    @staticmethod
    def get_accessible_data(user_id, api, share_id):
        accessible_data = policy_broker.get_user_api_info(user_id, api, share_id)
        return accessible_data

    # We add times to the following function to record the overheads

    def call_api(self,
                 api,
                 cur_user_id,
                 share_id,
                 exec_mode,
                 *args,
                 **kwargs):
        """
        Calls the API specified, ensuring that
          - data is only exposed to the API if permitted
          - data accessed by API is allowed

        Parameters:
         api: api to call
         cur_user_id: the user id to decide what data is exposed
         share_id: id of share from which the api is called,
         exec_mode: optimistic or pessimistic

        Returns:
         Response based on what happens
        """

        # print(trust_mode)

        # We first determine whether this is a data-blind function or data-aware function
        # data-aware function requires an argument called DE_id

        data_aware_flag = False
        data_aware_DE = set()

        if kwargs.__contains__("DE_id"):
            data_aware_flag = True
            data_aware_DE = set(kwargs["DE_id"])

        # print(data_aware_flag)
        # print(data_aware_DE)

        # First check if the share has been approved by all approval agents
        share_ready_flag = share_manager.check_share_ready(share_id)
        if not share_ready_flag:
            print("This share has not been approved to execute yet.")
            return None

        # If yes, set the accessible_de to be the entirety of P
        all_accessible_de_id = share_manager.get_de_ids_for_share(share_id)
        # print(f"all accessible data elements are: {all_accessible_de_id}")

        get_datasets_by_ids_res = database_api.get_datasets_by_ids(all_accessible_de_id)
        if get_datasets_by_ids_res.status == -1:
            err_msg = "No accessible data for " + api
            print(err_msg)
            return Response(status=1, message=err_msg)

        accessible_de = set()
        for cur_data in get_datasets_by_ids_res.data:
            if self.trust_mode == "no_trust":
                data_owner_symmetric_key = self.key_manager.get_agent_symmetric_key(cur_data.owner_id)
            else:
                data_owner_symmetric_key = None
            cur_de = DataElement(cur_data.id,
                                 cur_data.name,
                                 cur_data.type,
                                 cur_data.access_param,
                                 data_owner_symmetric_key)
            accessible_de.add(cur_de)

        # print(accessible_de)

        # actual api call
        ret = call_actual_api(api,
                              self.epf_path,
                              self.mount_dir,
                              accessible_de,
                              self.get_new_docker_id(),
                              self.docker_session,
                              *args,
                              )

        api_result = ret["return_info"][0]
        data_path_accessed = ret["return_info"][1]
        decryption_time = ret["return_info"][2]

        data_ids_accessed = []
        for path in data_path_accessed:
            data_ids_accessed.append(int(path.split("/")[-2]))
        # print("API result is", api_result)

        print("data accessed is", data_ids_accessed)
        # print("accessible data by policy is", accessible_data_policy)
        print("all accessible data is", all_accessible_de_id)
        # print("Decryption time is", decryption_time)

        if set(data_ids_accessed).issubset(set(all_accessible_de_id)):
            # print("All data access allowed by policy.")
            # log operation: logging intent_policy match
            self.data_station_log.log_intent_policy_match(cur_user_id,
                                                          api,
                                                          data_ids_accessed,
                                                          self.key_manager, )
            # In this case, we can return the result to caller.
            response = APIExecResponse(status=0,
                                       message="API result can be released",
                                       result=[api_result, decryption_time]
                                       )
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
        else:
            # TODO: illegal access can still happen since interceptor does not block access
            #  (except filter out inaccessible data when list dir)
            # print("Access to illegal data happened. Something went wrong")
            # log operation: logging intent_policy mismatch
            self.data_station_log.log_intent_policy_mismatch(cur_user_id,
                                                             api,
                                                             data_ids_accessed,
                                                             set(all_accessible_de_id),
                                                             self.key_manager, )
            response = Response(
                status=1, message="Access to illegal data happened. Something went wrong.")

        return response

    def shut_down(self):
        self.server.stop_server()


def call_actual_api(api_name,
                    epf_path,
                    mount_dir,
                    accessible_de,
                    docker_id,
                    docker_session:DSDocker,
                    *args,
                    **kwargs,
                    ):
    """
    The thread that runs the API on the Docker container

    Parameters:
     api_name: name of API to run on Docker container
     epf_path: path to the epf file
     mount_dir: directory of filesystem mount for Interceptor
     accessible_de: a set of accessible DataElement
     docker_id: id assigned to docker container
     server: flask server to receive communications with docker container
     *args / *kwargs for api

    Returns:
     Result of api
    """

    print(os.path.dirname(os.path.realpath(__file__)))
    # print(api_name, *args, **kwargs)
    epf_realpath = os.path.dirname(os.path.realpath(__file__)) + "/../" + epf_path

    config_dict = {"accessible_de": accessible_de, "docker_id": docker_id}
    print("The real epf path is", epf_realpath)

    # print(session.container.top())

    # run function
    list_of_functions = get_registered_functions()

    for cur_f in list_of_functions:
        if api_name == cur_f.__name__:
            print("call", api_name)
            docker_session.flask_run(api_name, epf_realpath, mount_dir, config_dict, *args, **kwargs)
            ret = docker_session.server.q.get(block=True)
            # print(ret)
            return ret

    # TODO clean up: uncomment line below in production
    # session.stop_and_prune()


# We add times to the following function to record the overheads

if __name__ == '__main__':
    print("Gatekeeper starting.")
