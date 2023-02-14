from dbservice import database_api
from common.pydantic_models.share import Share
from common.pydantic_models.user import User
from common.pydantic_models.response import Response, UploadShareResponse


def register_share_in_DB(cur_username,
                         share_id,
                         write_ahead_log=None,
                         key_manager=None,
                         ):

    # check if there is an existing user
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username,))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id

    # If in no_trust mode, we need to record this ADD_DATA to wal
    if write_ahead_log is not None:
        wal_entry = "database_api.create_share(Share(id=" + str(share_id) \
                    + "))"
        # If write_ahead_log is not None, key_manager also will not be None
        write_ahead_log.log(cur_user_id, wal_entry, key_manager, )

    new_share = Share(id=share_id)
    database_service_response = database_api.create_share(new_share)
    if database_service_response.status == -1:
        return Response(status=1, message="internal database error")

    return UploadShareResponse(status=0, message="success", share_id=share_id)
