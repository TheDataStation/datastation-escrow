
from os import path, remove, makedirs, removedirs
import random
import string

if __name__ == "__main__":
    for i in range(100):
        dst_file_path = 'file-' + str(i+1)
        f = open(dst_file_path, 'wb')
        N=10
        rnd_string = ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=N))

        f.write(bytes(rnd_string, 'utf-8'))
        f.close()
