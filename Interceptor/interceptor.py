#!/usr/bin/env python

#    Copyright (C) 2001  Jeff Epler  <jepler@unpythonic.dhs.org>
#    Copyright (C) 2006  Csaba Henk  <csaba.henk@creo.hu>
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

from __future__ import print_function

import os, sys
import pathlib
import socket
import time
from errno import *
from stat import *
import fcntl
from threading import Lock

# pull in some spaghetti to make this stuff work without fuse-py being installed
try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse
from fuse import Fuse

import threading
from pathlib import Path
from collections import defaultdict

# import mock_gatekeeper
# sys.path.append( '.' )
# import gatekeeper.gatekeeper
# sys.path.insert(0, str(pathlib.Path(os.getcwd())))
# print(str(pathlib.Path(os.getcwd()).parent))
# from gatekeeper import gatekeeper
# from dbservice.database_api import get_db
from crypto import cryptoutils

if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

fuse.feature_assert('stateful_files', 'has_init')


def flag2mode(flags):
    md = {os.O_RDONLY: 'rb', os.O_WRONLY: 'wb', os.O_RDWR: 'wb+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    # if 'a' in m and '+' not in m:
    #     m += '+'

    return m


class Xmp(Fuse):

    def __init__(self, *args, **kw):

        Fuse.__init__(self, *args, **kw)

        # do stuff to set up your filesystem here, if you want
        self.root = '/'

        # self.recv_end = connection_p

    #     self.accessible_data_paths = []
    #
    #     self.t = threading.Thread(target=self.mythread)
    #     self.t.start()
    #
    # def stop_thread(self):
    #     self.t.join()
    #
    # def mythread(self):
    #
    #     """
    #     The beauty of the FUSE python implementation is that with the python interp
    #     running in foreground, you can have threads
    #     """
    #     # print "mythread: started"
    #     while True:
    #         time.sleep(1)
    #         self.accessible_data_paths = connection_p.recv()
    #     #     time.sleep(120)
    #     #     print "mythread: ticking"

    def getattr(self, path):
        return os.lstat("." + path)

    def readlink(self, path):
        return os.readlink("." + path)

    def readdir(self, path, offset):
        # zz: filter out paths that are not the parent paths of any of the accessible data or itself

        pid = Fuse.GetContext(self)["pid"]
        in_other_process = False
        # print(pid, main_process_id_global, os.getpid())
        if pid not in accessible_data_dict_global.keys():
            in_other_process = True
        else:
            accessible_data_paths, symmetric_key = accessible_data_dict_global[pid]

        # print("Interceptor: readdir", path)
        path_to_access = pathlib.Path("." + path).absolute()
        # print(str(path_to_access))
        for e in os.listdir("." + path):
            # print(str(e))
            if in_other_process:
                yield fuse.Direntry(e)
            else:
                for acc_path in accessible_data_paths:
                    if path_to_access in pathlib.Path(acc_path).parents or str(path_to_access) == acc_path:
                        # print("Interceptor: yield")
                        yield fuse.Direntry(e)
                        break
                    # parts = pathlib.Path(acc_path).parts
                    # # print(parts)
                    # # TODO: prob too hacky
                    # if str(e) in parts:
                    #     # print("Interceptor: yield")
                    #     yield fuse.Direntry(e)
                    #     break

    def unlink(self, path):
        os.unlink("." + path)

    def rmdir(self, path):
        os.rmdir("." + path)

    def symlink(self, path, path1):
        os.symlink(path, "." + path1)

    def rename(self, path, path1):
        os.rename("." + path, "." + path1)

    def link(self, path, path1):
        os.link("." + path, "." + path1)

    def chmod(self, path, mode):
        os.chmod("." + path, mode)

    def chown(self, path, user, group):
        os.chown("." + path, user, group)

    def truncate(self, path, len):
        f = open("." + path, "a")
        f.truncate(len)
        f.close()

    def mknod(self, path, mode, dev):
        os.mknod("." + path, mode, dev)

    def mkdir(self, path, mode):
        os.mkdir("." + path, mode)

    def utime(self, path, times):
        os.utime("." + path, times)

    #    The following utimens method would do the same as the above utime method.
    #    We can't make it better though as the Python stdlib doesn't know of
    #    subsecond preciseness in acces/modify times.
    #
    #    def utimens(self, path, ts_acc, ts_mod):
    #      os.utime("." + path, (ts_acc.tv_sec, ts_mod.tv_sec))

    def access(self, path, mode):
        # mode_to_str = {0: "os.F_OK", 1: "os.X_OK", 2: "os.W_OK", 4: "os.R_OK"}
        # if mode == os.R_OK or mode == os.W_OK:
        # print("Interceptor: Testing access for " + path + " in " + mode_to_str[mode] + " mode")
        # print("Interceptor: fuse context:")
        # print(self.GetContext())
        # print(Fuse.GetContext(self))
        # TODO: maybe don't return error here (if accessing data that's shouldn't be accessible),
        #  since this requires the application to catch the error
        # access_okay = False
        path_to_access = pathlib.Path("." + path).absolute()
        # for acc_path in accessible_data_paths:
        #     if path_to_access in pathlib.Path(acc_path).parents or str(path_to_access) == acc_path:
        #         access_okay = True
        #         break
        # if mode != os.F_OK and not access_okay:
        #     return -EACCES
        if not os.access("." + path, mode):
            # if mode == os.R_OK:
            #     print("Interceptor: can't read")
            return -EACCES
        # else:
        #     if pathlib.Path(path_to_access).is_file():
        #         if mode != os.F_OK:
        #             fuse_context = Fuse.GetContext(self)
        #             pid = fuse_context["pid"]
        #
        #             if pid in accessible_data_dict_global.keys():
        #                 print("Interceptor: Access okay for " + str(path_to_access) + " in " + str(mode) + " mode")
        #                 print("Interceptor: pid:", pid)
        #
        #             if pid not in data_accessed_dict_global.keys():
        #                 data_accessed_dict_global[pid] = set()
        #
        #             cur_set = data_accessed_dict_global[pid]
        #             cur_set.add(str(path_to_access))
        #             data_accessed_dict_global[pid] = cur_set

                    # print(data_accessed_dict_global)
            # if mode == os.R_OK:
            #     print("Interceptor: read okay")

    #    This is how we could add stub extended attribute handlers...
    #    (We can't have ones which aptly delegate requests to the underlying fs
    #    because Python lacks a standard xattr interface.)
    #
    #    def getxattr(self, path, name, size):
    #        val = name.swapcase() + '@' + path
    #        if size == 0:
    #            # We are asked for size of the value.
    #            return len(val)
    #        return val
    #
    #    def listxattr(self, path, size):
    #        # We use the "user" namespace to please XFS utils
    #        aa = ["user." + a for a in ("foo", "bar")]
    #        if size == 0:
    #            # We are asked for size of the attr list, ie. joint size of attrs
    #            # plus null separators.
    #            return len("".join(aa)) + len(aa)
    #        return aa

    def statfs(self):
        """
        Should return an object with statvfs attributes (f_bsize, f_frsize...).
        Eg., the return value of os.statvfs() is such a thing (since py 2.2).
        If you are not reusing an existing statvfs object, start with
        fuse.StatVFS(), and define the attributes.

        To provide usable information (ie., you want sensible df(1)
        output, you are suggested to specify the following attributes:

            - f_bsize - preferred size of file blocks, in bytes
            - f_frsize - fundamental size of file blcoks, in bytes
                [if you have no idea, use the same as blocksize]
            - f_blocks - total number of blocks in the filesystem
            - f_bfree - number of free blocks
            - f_files - total number of file inodes
            - f_ffree - nunber of free file inodes
        """

        return os.statvfs(".")

    def fsinit(self):
        os.chdir(self.root)

    # zz: define the XmpFile class inside the function to the inner class can access methods of the outer class
    def getXmpFile(Xmp_self):

        class XmpFile(object):

            def __init__(self, path, flags, *mode):
                # currentThread = threading.current_thread()
                # dictionary = currentThread.__dict__
                # if "user_id" in dictionary.keys():
                #     print(dictionary["user_id"] + "is trying to open " + path + " in " + flag2mode(flags) + " mode")

                # if "user_id" in os.environ.keys():
                #     print("Interceptor: user_id = " + os.environ.get("user_id"))

                self.file_path = os.path.join(Path.cwd(), path[1:])
                self.file = os.fdopen(os.open("." + path, flags, *mode),
                                      flag2mode(flags))
                self.fd = self.file.fileno()

                if hasattr(os, 'pread'):
                    self.iolock = None
                else:
                    self.iolock = Lock()

                # uid, gid, pid = fuse_get_context()
                #
                # print(sys.argv[-1])

                # user_id = pathlib.PurePath(args[-1]).parts[-2]
                # api_name = pathlib.PurePath(args[-1]).parts[-1]

                # if mock_gatekeeper.check(user_id=user_id, api_name=api_name, file_to_access=self.file_path):
                #     print("Interceptor: Opened " + self.file_path + " in " + flag2mode(flags) + " mode")
                # else:
                #     self.file = None
                #     print("Interceptor: Access denied for " + self.file_path)
                #     raise IOError("Access denied for " + self.file_path)
                # print("Interceptor: Opened " + self.file_path + " in " + flag2mode(flags) + " mode")
                # data_id = gatekeeper.record_data_ids_accessed(self.file_path, user_id, api_name)
                # if data_id != None:
                #     data_ids_accessed.add(data_id)
                # f = open("/tmp/data_ids_accessed.txt", 'a+')
                # f.write(str(data_id) + '\n')
                # f.close()

                # print("Interceptor: data id accessed: ", str(data_id))
                # data_accessed.add(self.file_path)

                # zz: recording data accessed here
                 # TODO: in zero trust mode, should we record all access, including those illegal access with
                #      the wrong key?
                fuse_context = Fuse.GetContext(Xmp_self)
                pid = fuse_context["pid"]

                # if pid in accessible_data_dict_global.keys():
                #     print("Interceptor: Opened " + self.file_path + " in " + flag2mode(flags) + " mode")
                #     print("Interceptor: pid:", pid)

                if pid not in data_accessed_dict_global.keys():
                    data_accessed_dict_global[pid] = set()

                cur_set = data_accessed_dict_global[pid]
                cur_set.add(str(self.file_path))
                data_accessed_dict_global[pid] = cur_set

                self.truncate = False
                self.truncate_len = 0

            def read(self, length, offset):
                # print("Interceptor: I am reading " + str(self.file_path))

                # zz: get the symmetric key for the current user who runs the api's process,
                #  if the key is not None, then we know it's running in no trust mode.
                #  So we decrypt the data first and return the chunk of data the user is actually reading
                pid = Xmp_self.GetContext()["pid"]

                symmetric_key = None
                if pid in accessible_data_dict_global.keys():
                    symmetric_key = accessible_data_dict_global[pid][1]

                # if self.file != None:
                if self.iolock:
                    self.iolock.acquire()
                    try:
                        if symmetric_key is not None:
                            encrypted_bytes = self.file.read()
                            decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                                ciphertext=encrypted_bytes,
                                key=symmetric_key)
                            # TODO: what happens if decryption fails? For now just return an empty byte.
                            #  We can't return something that's larger than length (the size of buffer we are trying to read)
                            if decrypted_bytes is not None:
                                return decrypted_bytes[offset:offset + length]
                            else:
                                print("Interceptor: Cannot decrypt ", self.file_path)
                                return b''
                        else:
                            self.file.seek(offset)
                            return self.file.read(length)
                    finally:
                        self.iolock.release()
                else:
                    if symmetric_key is not None:
                        encrypted_bytes = os.pread(self.fd, os.stat(self.file_path).st_size, 0)
                        decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                            ciphertext=encrypted_bytes,
                            key=symmetric_key)
                        if decrypted_bytes is not None:
                            # if offset >= len(decrypted_bytes):
                            #     return b''
                            # else:
                            return decrypted_bytes[offset:offset + length]
                        else:
                            print("Interceptor: Cannot decrypt ", self.file_path)
                            return b''
                    else:
                        return os.pread(self.fd, length, offset)
                # else:
                #     raise IOError("Read access denied for " + self.file_path)

            def write(self, buf, offset):
                # print("Interceptor: I am writing " + str(self.file_path))
                # print("Interceptor: buf:")
                # print(str(type(buf)))
                # print(buf.decode())

                pid = Xmp_self.GetContext()["pid"]

                symmetric_key = None
                if pid in accessible_data_dict_global.keys():
                    symmetric_key = accessible_data_dict_global[pid][1]

                if self.iolock:
                    self.iolock.acquire()
                    try:
                        if symmetric_key is not None:
                            encrypted_bytes = self.file.read()
                            decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                                ciphertext=encrypted_bytes,
                                key=symmetric_key)
                            if decrypted_bytes == None:
                                print("Interceptor: Cannot decrypt ", self.file_path)
                                return 0

                            # if self.truncate:
                            #     print("Interceptor: truncate")
                                # decrypted_bytes = decrypted_bytes[:self.truncate_len]
                                # self.truncate = False

                            new_bytes = decrypted_bytes[:offset] + buf
                            if offset + len(buf) < len(decrypted_bytes):
                                new_bytes += decrypted_bytes[offset + len(buf):]
                            # print("Interceptor: new_bytes:")
                            # print(cryptoutils.from_bytes(new_bytes))
                            # print(new_bytes.decode())
                            # new_bytes = decrypted_bytes[:offset] + buf + decrypted_bytes[offset + len(buf) : ]
                            new_encrypted_bytes = cryptoutils.encrypt_data_with_symmetric_key(
                                data=new_bytes,
                                key=symmetric_key
                            )
                            if new_encrypted_bytes is not None:
                                self.file.truncate(0)
                                self.file.write(new_encrypted_bytes)
                                return len(buf)
                            else:
                                print("Interceptor: Cannot encrypt ", self.file_path)
                                # self.file.write(encrypted_bytes)
                                return 0
                        else:
                            self.file.seek(offset)
                            self.file.write(buf)
                            return len(buf)
                    finally:
                        self.iolock.release()
                else:
                    if symmetric_key is not None:
                        # print("Interceptor: in")
                        encrypted_bytes = os.pread(self.fd, os.stat(self.file_path).st_size, 0)
                        # print("Interceptor: encrypted_bytes:")
                        # print(encrypted_bytes.decode())
                        decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                            ciphertext=encrypted_bytes,
                            key=symmetric_key)
                        if decrypted_bytes == None:
                            print("Interceptor: Cannot decrypt ", self.file_path)
                            return 0
                        # print("Interceptor: decrypted_bytes:")
                        # print(decrypted_bytes)
                        # print(decrypted_bytes.decode())

                        # decrypted_bytes = bytearray(decrypted_bytes)
                        # print(decrypted_bytes.decode().split("\n")[0])
                        # if self.truncate:
                            # print("Interceptor: truncate")
                            # decrypted_bytes = decrypted_bytes[:self.truncate_len]
                        # print(decrypted_bytes.decode())
                        # print("Interceptor: buf:")
                        # print(buf)
                        new_bytes = decrypted_bytes[:offset] + buf
                        # print("Interceptor: decrypted_bytes[:offset]:")
                        # print(decrypted_bytes[:offset].decode())
                        # print("Interceptor: buf:")
                        # print(buf)
                        # print(buf[:13].decode())
                        # print(buf.decode())
                        if offset + len(buf) < len(decrypted_bytes):
                            new_bytes += decrypted_bytes[offset + len(buf):]
                        # print("Interceptor: new_bytes:")
                        # print(cryptoutils.from_bytes(new_bytes))
                        # print(new_bytes.decode())
                        # print(new_bytes.decode().split("\n")[0])

                        new_encrypted_bytes = cryptoutils.encrypt_data_with_symmetric_key(
                            data=new_bytes,
                            key=symmetric_key
                        )
                        if new_encrypted_bytes is not None:
                            self.file.truncate(0)
                            os.pwrite(self.fd, new_encrypted_bytes, 0)
                            return len(buf)
                        else:
                            print("Interceptor: Cannot encrypt ", self.file_path)
                            # os.pwrite(self.fd, encrypted_bytes, 0)
                            return 0
                    else:
                        return os.pwrite(self.fd, buf, offset)

            def release(self, flags):
                self.file.close()
                # print("Interceptor: release " + str(self.file_path))

            def _fflush(self):
                if 'w' in self.file.mode or 'a' in self.file.mode:
                    self.file.flush()

            def fsync(self, isfsyncfile):
                self._fflush()
                if isfsyncfile and hasattr(os, 'fdatasync'):
                    os.fdatasync(self.fd)
                else:
                    os.fsync(self.fd)
                # print("Interceptor: fsync " + str(self.file_path))

            def flush(self):
                self._fflush()
                # cf. xmp_flush() in fusexmp_fh.c
                os.close(os.dup(self.fd))
                # print("Interceptor: flush " + str(self.file_path))

            def fgetattr(self):
                print("Interceptor: fgetattr " + str(self.file_path))
                return os.fstat(self.fd)

            def ftruncate(self, trunc_len):
                print("Interceptor: ftruncate " + str(self.file_path) + " with length " + str(trunc_len))

                pid = Xmp_self.GetContext()["pid"]

                symmetric_key = None
                if pid in accessible_data_dict_global.keys():
                    symmetric_key = accessible_data_dict_global[pid][1]

                if symmetric_key is not None:
                    # self.truncate = True
                    # self.truncate_len = trunc_len
                    if self.iolock:
                        self.iolock.acquire()
                        try:
                            encrypted_bytes = self.file.read()
                            decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                                ciphertext=encrypted_bytes,
                                key=symmetric_key)
                            if decrypted_bytes == None:
                                print("Interceptor: Cannot decrypt ", self.file_path)
                                return

                            if trunc_len > len(decrypted_bytes):
                                new_bytes = decrypted_bytes
                            else:
                                new_bytes = decrypted_bytes[:trunc_len]
                            new_encrypted_bytes = cryptoutils.encrypt_data_with_symmetric_key(
                                data=new_bytes,
                                key=symmetric_key
                            )
                            if new_encrypted_bytes is not None:
                                self.file.truncate()
                                num_bytes_wrote = self.file.write(new_encrypted_bytes)
                                assert num_bytes_wrote == len(new_encrypted_bytes)
                                self.fsync(isfsyncfile=True)
                            else:
                                print("Interceptor: Cannot encrypt ", self.file_path)
                                # self.file.write(encrypted_bytes)
                        finally:
                            self.iolock.release()
                    else:
                        encrypted_bytes = os.pread(self.fd, os.stat(self.file_path).st_size, 0)
                        decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                            ciphertext=encrypted_bytes,
                            key=symmetric_key)
                        if decrypted_bytes == None:
                            print("Interceptor: Cannot decrypt ", self.file_path)
                            return

                        if trunc_len > len(decrypted_bytes):
                            new_bytes = decrypted_bytes
                        else:
                            new_bytes = decrypted_bytes[:trunc_len]
                        # print("Interceptor: new_bytes:")
                        # print(new_bytes)
                        # print(new_bytes.decode())
                        new_encrypted_bytes = cryptoutils.encrypt_data_with_symmetric_key(
                            data=new_bytes,
                            key=symmetric_key
                        )
                        # print("Interceptor: new_encrypted_bytes:")
                        # print(new_encrypted_bytes)
                        # print(cryptoutils.decrypt_data_with_symmetric_key(
                        #     ciphertext=new_encrypted_bytes,
                        #     key=symmetric_key))
                        if new_encrypted_bytes is not None:
                            self.file.truncate()
                            num_bytes_wrote = os.pwrite(self.fd, new_encrypted_bytes, 0)
                            assert num_bytes_wrote == len(new_encrypted_bytes)
                            # num_bytes_wrote = self.file.write(new_encrypted_bytes)
                            # print("Interceptor: num_bytes_wrote:", num_bytes_wrote)
                            self.fsync(isfsyncfile=True)
                            # encrypted_bytes = os.pread(self.fd, os.stat(self.file_path).st_size, 0)
                            # decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                            #     ciphertext=encrypted_bytes,
                            #     key=symmetric_key)
                            # print("Interceptor: decrypted_bytes:")
                            # print(decrypted_bytes.decode())
                        else:
                            print("Interceptor: Cannot encrypt ", self.file_path)
                            # os.pwrite(self.fd, encrypted_bytes, 0)
                else:
                    self.file.truncate(trunc_len)

            def lock(self, cmd, owner, **kw):
                # The code here is much rather just a demonstration of the locking
                # API than something which actually was seen to be useful.

                # Advisory file locking is pretty messy in Unix, and the Python
                # interface to this doesn't make it better.
                # We can't do fcntl(2)/F_GETLK from Python in a platfrom independent
                # way. The following implementation *might* work under Linux.
                #
                # if cmd == fcntl.F_GETLK:
                #     import struct
                #
                #     lockdata = struct.pack('hhQQi', kw['l_type'], os.SEEK_SET,
                #                            kw['l_start'], kw['l_len'], kw['l_pid'])
                #     ld2 = fcntl.fcntl(self.fd, fcntl.F_GETLK, lockdata)
                #     flockfields = ('l_type', 'l_whence', 'l_start', 'l_len', 'l_pid')
                #     uld2 = struct.unpack('hhQQi', ld2)
                #     res = {}
                #     for i in xrange(len(uld2)):
                #          res[flockfields[i]] = uld2[i]
                #
                #     return fuse.Flock(**res)

                # Convert fcntl-ish lock parameters to Python's weird
                # lockf(3)/flock(2) medley locking API...
                op = {fcntl.F_UNLCK: fcntl.LOCK_UN,
                      fcntl.F_RDLCK: fcntl.LOCK_SH,
                      fcntl.F_WRLCK: fcntl.LOCK_EX}[kw['l_type']]
                if cmd == fcntl.F_GETLK:
                    return -EOPNOTSUPP
                elif cmd == fcntl.F_SETLK:
                    if op != fcntl.LOCK_UN:
                        op |= fcntl.LOCK_NB
                elif cmd == fcntl.F_SETLKW:
                    pass
                else:
                    return -EINVAL

                fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])

        return XmpFile

    def main(self, *a, **kw):

        self.file_class = self.getXmpFile()

        return Fuse.main(self, *a, **kw)


def main(root_dir, mount_point, accessible_data_dict, data_accessed_dict):
    # engine.dispose()

    global args
    # run in foreground
    args = ["-f", "-o", "root=" + root_dir, mount_point]
    # run in background
    # args = ["-o", "root=" + root_dir, mount_point]

    global accessible_data_dict_global
    accessible_data_dict_global = accessible_data_dict
    global data_accessed_dict_global
    data_accessed_dict_global = data_accessed_dict
    global symmetric_key_dict_global
    symmetric_key_dict_global = {}

    usage = """
Userspace nullfs-alike: mirror the filesystem tree from some point on.

""" + Fuse.fusage

    server = Xmp(
        version="%prog " + fuse.__version__,
        usage=usage,
        dash_s_do='setsingle')

    server.parser.add_option(mountopt="root", metavar="PATH", default='/',
                             help="mirror filesystem from under PATH [default: %default]")

    result = server.parse(args=args, values=server, errex=1)

    try:
        if server.fuse_args.mount_expected():
            os.chdir(server.root)
    except OSError:
        print("Interceptor: can't enter root of underlying filesystem", file=sys.stderr)
        sys.exit(1)

    server.main()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], [])
