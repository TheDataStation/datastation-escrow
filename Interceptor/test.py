import pathlib
import sys
import threading
import os
import subprocess

import interceptor
import mock_api

if __name__ == '__main__':
    # currentThread = threading.current_thread()
    # dictionary = currentThread.__dict__
    # dictionary["user_id"] = "zhiru"

    # os.environ["user_id"] = "zhiru"
    # if "user_id" in os.environ.keys():
    #     print("user_id = " + os.environ.get("user_id"))

    data_path = "/Users/zhiruzhu/Desktop/data_station/DataStation/Interceptor/test"
    ds_workspace = "/Users/zhiruzhu/Desktop/data_station/DataStation/Interceptor/test_mount"

    # zz: create a mount point inside data station's workspace that encodes user_id and api_name,
    #  so the interceptor can detect and pass them to the gatekeeper
    # create nested directory for now, but we can combine user_id and api_name to a single directory name
    # using some encryption
    user_id = "zhiru"
    api_name = "union_all_files"
    mount_point = os.path.join(ds_workspace, user_id, api_name)
    pathlib.Path(mount_point).mkdir(parents=True, exist_ok=True)

    # zz: run the interceptor, mount the data_path to mount_point
    interceptor_path = "/Users/zhiruzhu/Desktop/data_station/DataStation/Interceptor/interceptor.py"
    subprocess.call( #-s -o root=
        "python " + interceptor_path + " " + data_path + " " + mount_point,
        shell=True)
    # interceptor.main(root_dir=data_path, mount_point=mount_point)

    # zz: call the api, note the api should access data in mount_point in data station's workspace
    mock_api.union_all_files(mount_point)

    # zz: check result (it should contain the content of all accessible files)
    # with open(os.path.join(mount_point, "staging/result.txt"), "r") as f:
        # print(f.read())
        # print(f.readlines())

    # zz: unmount immediately after we're done
    os.system("umount " + mount_point)
