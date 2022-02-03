import pathlib
import os

def union_all_files(dir_path):
    # combines all (accessible) files into a new file in staging dir
    files = pathlib.Path(dir_path).glob('*.txt')
    with open(os.path.join(dir_path, "staging/result.txt"), "w+") as new_file:
        for file in files:
            if file.is_file() and "result" not in file.name:
                try:
                    with open(file, "r") as cur_file:
                        new_file.write(cur_file.read() + "\n")
                        cur_file.close()
                except Exception as e:
                    print("IO error on " + str(file) + ": " + str(e))
                    raise
                # with open(file, "r") as cur_file:
                #     new_file.write(cur_file.read() + "\n")
                #     cur_file.close()

def read_all_files(dir_path):
    files = pathlib.Path(dir_path).glob('*.txt')
    for file in files:
        if file.is_file():
            with open(file, "r") as cur_file:
                print(cur_file.read())
                cur_file.close()

def write_to_files(dir_path):
    files = pathlib.Path(dir_path).glob('*.txt')
    for file in files:
        if file.is_file():
            try:
                with open(file, "a") as cur_file:
                    cur_file.write("hello")
                    cur_file.close()
            except Exception as e:
                print("write error on " + str(file) + ": " + str(e))

if __name__ == '__main__':
    mount_point = "/Users/zhiruzhu/Desktop/data_station/DataStation/Interceptor/test_mount/zhiru/union_all_files"
    union_all_files(mount_point)
    with open(os.path.join(mount_point, "staging/result.txt"), "r") as f:
        # print(f.read())
        print(f.readlines())