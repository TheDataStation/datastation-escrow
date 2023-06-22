import json

from dbservice import database_api
from common.pydantic_models.share import Share
from common.pydantic_models.user import User
from common.pydantic_models.response import Response, UploadShareResponse


def register_share_in_DB(cur_username,
                         share_id,
                         dest_agents,
                         data_elements,
                         template,
                         *args,
                         **kwargs
                         ):
    # check if there is an existing user
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")

    # First add to the Share table
    param_json = {"args": args, "kwargs": kwargs}
    param_str = json.dumps(param_json)

    database_service_response = database_api.create_share(share_id, template, param_str)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    # Then add to ShareDest table and ShareDE table
    # TODO: from here

    return UploadShareResponse(status=0, message="success", share_id=share_id)


def register_share_in_DB_no_trust(cur_username,
                                  share_id,
                                  dest_agents,
                                  data_elements,
                                  template,
                                  write_ahead_log,
                                  key_manager,
                                  *args,
                                  **kwargs,
                                  ):
    # check if there is an existing user
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username, ))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    param_json = {"args": args, "kwargs": kwargs}
    param_str = json.dumps(param_json)

    wal_entry = f"database_api.create_share({str(share_id)}, {template}, {param_str})"
    write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    database_service_response = database_api.create_share(share_id, template, param_str)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    return UploadShareResponse(status=0, message="success", share_id=share_id)
