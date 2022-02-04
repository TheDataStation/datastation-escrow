def check(user_id, api_name, file_to_access):
    # need to get data id from file_to_access, which is an absolute path of the file to be accessed
    # then check if the request matches any policy
    # print(user_id, api_name, file_to_access)
    if user_id == "zhiru":
        if api_name == "union_all_files" or api_name == "read_all_files" or api_name == "write_to_files":
            if "test1" in file_to_access:
                return False
    return True
