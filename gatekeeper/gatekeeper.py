import os
from dsapplicationregistration.dsar_core import (register_epf,
                                                 get_api_endpoint_names,
                                                 get_functions_names,
                                                 get_registered_functions, )
from dbservice import database_api
from policybroker import policy_broker
from common.pydantic_models.api import API
from common.pydantic_models.api_dependency import APIDependency
from common.pydantic_models.user import User
from common.pydantic_models.response import Response, APIExecResponse

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
                 ):
        """
        The general class for the gatekeeper, which brokers access to data elements
        """

        # save variables
        self.data_station_log = data_station_log
        self.write_ahead_log = write_ahead_log
        self.key_manager = key_manager
        self.trust_mode = trust_mode

        self.epf_path = epf_path
        self.mount_dir = mount_dir
        # set docker id variable
        self.docker_id = 1

        self.server = FlaskDockerServer()
        self.server.start_server()

        # print("Start setting up the gatekeeper")
        print("Start setting up the gatekeeper")
        register_epf(epf_path)
        procedure_names = get_api_endpoint_names()
        function_names = get_functions_names()
        print(procedure_names)
        print(function_names)
        functions = get_registered_functions()
        # dependencies_to_register = get_registered_dependencies()
        # # print(dependencies_to_register)
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

        print("Gatekeeper setup success")

    def get_new_docker_id(self):
        ret = self.docker_id
        self.docker_id += 1
        return ret

    def get_accessible_data(self, user_id, api, share_id):
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

        # look at the accessible data by policy for current (user, api)
        # print(cur_user_id, api)
        accessible_data_policy = self.get_accessible_data(cur_user_id, api, share_id)

        # Note: In data-aware-functions, if accessible_data_policy does not include data_aware_DE,
        # we end the execution immediately
        if not data_aware_DE.issubset(set(accessible_data_policy)):
            err_msg = "Attempted to access data not allowed by policies. Execution stops."
            print(err_msg)
            return Response(status=1, message=err_msg)

        # look at all optimistic data from the DB
        optimistic_data = database_api.get_all_optimistic_datasets()
        accessible_data_optimistic = []
        for i in range(len(optimistic_data.data)):
            cur_optimistic_id = optimistic_data.data[i].id
            accessible_data_optimistic.append(cur_optimistic_id)

        # Combine these two types of accessible data elements together
        # In optimistic execution mode, we include optimistic datasets as well
        if exec_mode == "optimistic":
            all_accessible_data_id = set(
                accessible_data_policy + accessible_data_optimistic)
        # In pessimistic execution mode, we only include data that are allowed by policies
        elif not data_aware_flag:
            all_accessible_data_id = set(accessible_data_policy)
        # Lastly, in data-aware execution, all_accessible_data_id should be data_aware_DE
        else:
            all_accessible_data_id = data_aware_DE
        print("all accessible data elements are: ", all_accessible_data_id)

        get_datasets_by_ids_res = database_api.get_datasets_by_ids(
            all_accessible_data_id)
        if get_datasets_by_ids_res.status == -1:
            err_msg = "No accessible data for " + api
            print(err_msg)
            return Response(status=1, message=err_msg)
        accessible_data_paths = set(
            [dataset.access_type for dataset in get_datasets_by_ids_res.data])

        # if in zero trust mode, send user's symmetric key to interceptor in order to decrypt files
        trust_mode = self.trust_mode

        accessible_data_key_dict = {}
        if trust_mode == "no_trust":
            # get the symmetric key of each accessible data's owner,
            # and store them in dict to pass to interceptor
            accessible_data_key_dict = {}
            for dataset in get_datasets_by_ids_res.data:
                data_owner_symmetric_key = self.key_manager.get_agent_symmetric_key(
                    dataset.owner_id)
                accessible_data_key_dict[dataset.access_type] = data_owner_symmetric_key

        # Update path names

        accessible_data_old_set = accessible_data_paths
        accessible_data_new_set = set()
        for cur_data in accessible_data_old_set:
            data_str_list = cur_data.split("/")[-2:]
            cur_data = os.path.join("/mnt/data", data_str_list[0], data_str_list[1])
            accessible_data_new_set.add(cur_data)

        key_map = dict()
        for cur_key in accessible_data_key_dict:
            data_str_list = cur_key.split("/")[-2:]
            new_key = os.path.join("/mnt/data", data_str_list[0], data_str_list[1])
            key_map[cur_key] = new_key

        accessible_data_key_dict_new = {newkey: accessible_data_key_dict[oldkey]
                                        for (oldkey, newkey) in key_map.items()}

        accessible_data_dict = (accessible_data_new_set, accessible_data_key_dict_new)

        # actual api call
        ret = call_actual_api(api,
                              self.epf_path,
                              self.mount_dir,
                              accessible_data_dict,
                              self.get_new_docker_id(),
                              self.server,
                              *args,
                              )

        api_result = ret["return_value"]
        data_path_accessed = api_result[1]
        data_ids_accessed = []
        for path in data_path_accessed:
            data_ids_accessed.append(int(path.split("/")[-2]))
        api_result = api_result[0]
        print("API result is", api_result)

        print("data accessed is", data_ids_accessed)
        print("accessible data by policy is", accessible_data_policy)
        print("all accessible data is", all_accessible_data_id)

        if set(data_ids_accessed).issubset(set(accessible_data_policy)):
            # print("All data access allowed by policy.")
            # log operation: logging intent_policy match
            self.data_station_log.log_intent_policy_match(cur_user_id,
                                                          api,
                                                          data_ids_accessed,
                                                          self.key_manager, )
            # In this case, we can return the result to caller.
            response = APIExecResponse(status=0,
                                       message="API result can be released",
                                       result=api_result,
                                       )
        elif set(data_ids_accessed).issubset(all_accessible_data_id):
            # print("Some access to optimistic data not allowed by policy.")
            # log operation: logging intent_policy mismatch
            self.data_station_log.log_intent_policy_mismatch(cur_user_id,
                                                             api,
                                                             data_ids_accessed,
                                                             set(accessible_data_policy),
                                                             self.key_manager, )
            response = APIExecResponse(status=-1,
                                       message="Some access to optimistic data not allowed by policy.",
                                       result=[api_result, data_ids_accessed], )
        else:
            # TODO: illegal access can still happen since interceptor does not block access
            #  (except filter out inaccessible data when list dir)
            # print("Access to illegal data happened. Something went wrong")
            # log operation: logging intent_policy mismatch
            self.data_station_log.log_intent_policy_mismatch(cur_user_id,
                                                             api,
                                                             data_ids_accessed,
                                                             set(accessible_data_policy),
                                                             self.key_manager, )
            response = Response(
                status=1, message="Access to illegal data happened. Something went wrong.")

        return response

    def shut_down(self):
        self.server.stop_server()


def call_actual_api(api_name,
                    epf_path,
                    mount_dir,
                    accessible_data_dict,
                    docker_id,
                    server,
                    *args,
                    **kwargs,
                    ):
    """
    The thread that runs the API on the Docker container

    Parameters:
     api_name: name of API to run on Docker container
     epf_path: path to the epf file
     mount_dir: directory of filesystem mount for Interceptor
     accessible_data_dict: dictionary of data that API is allowed to access, fed to Interceptor
     docker_id: id assigned to docker container
     server: flask server to receive communications with docker container
     *args / *kwargs for api

    Returns:
     Result of api
    """

    print(os.path.dirname(os.path.realpath(__file__)))
    # print(api_name, *args, **kwargs)
    # print(os.path.dirname(os.path.realpath(__file__)))
    # print(api_name, *args, **kwargs)
    # print("list_of_apis:", list_of_apis)
    # time.sleep(1)
    # print("connector name / module path: ", connector_name, connector_module_path)
    # print("accessed path: " + os.path.dirname(os.path.realpath(__file__)) + "/../" + connector_module_path,)
    epf_realpath = os.path.dirname(os.path.realpath(__file__)) + "/../" + epf_path
    docker_image_realpath = os.path.dirname(os.path.realpath(__file__)) + "/../" + "ds_dev_utils/docker/images"

    config_dict = {"accessible_data_dict": accessible_data_dict, "docker_id": docker_id}
    print("The real epf path is", epf_realpath)
    session = DSDocker(
        server,
        epf_realpath,
        mount_dir,
        config_dict,
        docker_image_realpath,
    )

    # print(session.container.top())

    # run function
    list_of_functions = get_registered_functions()

    for cur_f in list_of_functions:
        if api_name == cur_f.__name__:
            print("call", api_name)
            session.flask_run(api_name, *args, **kwargs)
            ret = server.q.get(block=True)
            print(ret)
            return ret

    # TODO clean up: uncomment line below in production
    # session.stop_and_prune()


# We add times to the following function to record the overheads

if __name__ == '__main__':
    print("Gatekeeper starting.")
