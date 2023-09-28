from dbservice import database_api


def verify_de_owner(de_id, user_id):
    # get data element owner id
    de_owner_id = database_api.get_de_owner(de_id)
    if de_owner_id is None:
        return {"status": 1, "message": "Error retrieving data owner."}
    # print("Dataset owner id is: " + str(dataset_owner_id))

    # print("User id is", user_id)
    # print("Owner id is", dataset_owner_id)
    if int(user_id) != int(de_owner_id):
        print(user_id == de_owner_id)
        return {"status": 1, "message": "Current user is not owner of dataset"}

    return {"status": 0, "message": "verifying DE owner success"}
