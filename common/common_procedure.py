from dbservice import database_api
from models.dataset import *
from models.user import *
from models.response import *


def verify_dataset_owner(dataset_id, cur_username):
    # get dataset owner id
    dataset_owner = database_api.get_dataset_owner(Dataset(id=dataset_id,))
    if dataset_owner.status == -1:
        return Response(status=1, message="Error retrieving data owner.")
    dataset_owner_id = dataset_owner.data[0].id
    # print("Dataset owner id is: " + str(dataset_owner_id))

    # get current user id
    cur_user = database_api.get_user_by_user_name(User(user_name=cur_username,))
    # If the user doesn't exist, something is wrong
    if cur_user.status == -1:
        return Response(status=1, message="Something wrong with the current user")
    cur_user_id = cur_user.data[0].id
    # print("Current user id is: "+str(cur_user_id))

    if cur_user_id != dataset_owner_id:
        return Response(status=1, message="Current user is not owner of dataset")

    return Response(status=0, message="verification success")
