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

# import mock_gatekeeper
# sys.path.append( '.' )
# import gatekeeper.gatekeeper
# sys.path.insert(0, str(pathlib.Path(os.getcwd())))
# print(str(pathlib.Path(os.getcwd()).parent))
# from gatekeeper import gatekeeper
# from dbservice.database_api import get_db

if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

fuse.feature_assert('stateful_files', 'has_init')


def flag2mode(flags):
    md = {os.O_RDONLY: 'rb', os.O_WRONLY: 'wb', os.O_RDWR: 'wb+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m


class Xmp(Fuse):

    def __init__(self, *args, **kw):

        Fuse.__init__(self, *args, **kw)

        # do stuff to set up your filesystem here, if you want
        # import thread
        # thread.start_new_thread(self.mythread, ())
        self.root = '/'

    #    def mythread(self):
    #
    #        """
    #        The beauty of the FUSE python implementation is that with the python interp
    #        running in foreground, you can have threads
    #        """
    #        print "mythread: started"
    #        while 1:
    #            time.sleep(120)
    #            print "mythread: ticking"

    def getattr(self, path):
        return os.lstat("." + path)

    def readlink(self, path):
        return os.readlink("." + path)

    def readdir(self, path, offset):
        # print("readdir", path)
        path_to_access = pathlib.Path("." + path).absolute()
        # print(str(path_to_access))
        for e in os.listdir("." + path):
            # print(str(e))
            for acc_path in accessible_data_paths:
                if path_to_access in pathlib.Path(acc_path).parents or str(path_to_access) == acc_path:
                    # print("yield")
                    yield fuse.Direntry(e)
                    break
                # parts = pathlib.Path(acc_path).parts
                # # print(parts)
                # # TODO: prob too hacky
                # if str(e) in parts:
                #     # print("yield")
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
        # print("I am accessing " + path)
        if not os.access("." + path, mode):
            return -EACCES

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

    class XmpFile(object):

        def __init__(self, path, flags, *mode):
            # currentThread = threading.current_thread()
            # dictionary = currentThread.__dict__
            # if "user_id" in dictionary.keys():
            #     print(dictionary["user_id"] + "is trying to open " + path + " in " + flag2mode(flags) + " mode")

            # if "user_id" in os.environ.keys():
            #     print("user_id = " + os.environ.get("user_id"))

            self.file_path = os.path.join(Path.cwd(), path[1:])
            self.file = os.fdopen(os.open("." + path, flags, *mode),
                                  flag2mode(flags))
            self.fd = self.file.fileno()

            if hasattr(os, 'pread'):
                self.iolock = None
            else:
                self.iolock = Lock()

            # print(sys.argv[-1])

            # user_id = pathlib.PurePath(args[-1]).parts[-2]
            # api_name = pathlib.PurePath(args[-1]).parts[-1]

            # if mock_gatekeeper.check(user_id=user_id, api_name=api_name, file_to_access=self.file_path):
            #     print("Opened " + self.file_path + " in " + flag2mode(flags) + " mode")
            # else:
            #     self.file = None
            #     print("Access denied for " + self.file_path)
            #     raise IOError("Access denied for " + self.file_path)
            print("Opened " + self.file_path + " in " + flag2mode(flags) + " mode")
            # data_id = gatekeeper.record_data_ids_accessed(self.file_path, user_id, api_name)
            # if data_id != None:
            #     data_ids_accessed.add(data_id)
            # f = open("/tmp/data_ids_accessed.txt", 'a+')
            # f.write(str(data_id) + '\n')
            # f.close()

            # print("data id accessed: ", str(data_id))
            data_accessed.add(self.file_path)

        def read(self, length, offset):
            if self.file != None:
                if self.iolock:
                    self.iolock.acquire()
                    try:
                        self.file.seek(offset)
                        print("I am reading " + str(self.file_path))
                        return self.file.read(length)
                    finally:
                        self.iolock.release()
                else:
                    print("I am reading " + str(self.file_path))
                    return os.pread(self.fd, length, offset)
            # else:
            #     raise IOError("Read access denied for " + self.file_path)

        def write(self, buf, offset):
            if self.file != None:
                # print("I am writing " + str(self.file_path))
                if self.iolock:
                    self.iolock.acquire()
                    try:
                        self.file.seek(offset)
                        self.file.write(buf)
                        print("I am writing " + str(self.file_path))
                        return len(buf)
                    finally:
                        self.iolock.release()
                else:
                    print("I am writing " + str(self.file_path))
                    return os.pwrite(self.fd, buf, offset)

        def release(self, flags):
            if self.file != None:
                self.file.close()
                print("release " + str(self.file_path))

        def _fflush(self):
            if 'w' in self.file.mode or 'a' in self.file.mode:
                self.file.flush()

        def fsync(self, isfsyncfile):
            if self.file != None:
                self._fflush()
                if isfsyncfile and hasattr(os, 'fdatasync'):
                    os.fdatasync(self.fd)
                else:
                    os.fsync(self.fd)
                print("fsync " + str(self.file_path))

        def flush(self):
            if self.file != None:
                self._fflush()
                # cf. xmp_flush() in fusexmp_fh.c
                os.close(os.dup(self.fd))
                print("flush " + str(self.file_path))

        def fgetattr(self):
            if self.file != None:
                print("fgetattr " + str(self.file_path))
                return os.fstat(self.fd)

        def ftruncate(self, len):
            if self.file != None:
                print("ftruncate " + str(self.file_path))
                self.file.truncate(len)

        def lock(self, cmd, owner, **kw):
            if self.file != None:
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

    def main(self, *a, **kw):

        self.file_class = self.XmpFile

        return Fuse.main(self, *a, **kw)


def main(root_dir, mount_point, accessible_data, send_end):
    # engine.dispose()

    global args
    # run in foreground
    # args = ["-s", "-f", "-o", "root="+root_dir, mount_point]
    # run in background
    args = ["-s", "-o", "root=" + root_dir, mount_point]

    global data_accessed
    data_accessed = set()

    global accessible_data_paths
    accessible_data_paths = accessible_data

    # global accessible_data_paths
    # accessible_data_paths = []
    # with open("/tmp/accessible_data_paths.txt", "r") as f:
    #     content = f.read()
    #     if len(content) != 0:
    #         accessible_data_paths = content.split("\n")[:-1]
    # print("accessible data paths:")
    # print(accessible_data_paths)
    # os.remove("/tmp/accessible_data_paths.txt")

    # host = "localhost"
    # port = 6666
    # sock = socket.socket()
    # sock.bind((host, port))
    # sock.listen(1)

    # print(args)

    usage = """
Userspace nullfs-alike: mirror the filesystem tree from some point on.

""" + Fuse.fusage

    server = Xmp(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')

    server.parser.add_option(mountopt="root", metavar="PATH", default='/',
                             help="mirror filesystem from under PATH [default: %default]")
    # server.root = "/Users/zhiruzhu/Desktop/data_station/Interceptor/test"
    # server.fuse_args.mountpoint = "/Users/zhiruzhu/Desktop/data_station/Interceptor/test_mount/zhiru/union_all_files"
    # server.parser.fuse_args.mountpoint = "/Users/zhiruzhu/Desktop/data_station/Interceptor/test_mount/zhiru" \
    #                                      "/union_all_files"

    result = server.parse(args=args, values=server, errex=1)
    # print(result)

    try:
        if server.fuse_args.mount_expected():
            os.chdir(server.root)
    except OSError:
        print("can't enter root of underlying filesystem", file=sys.stderr)
        sys.exit(1)

    server.main()

    # print("Data ids accessed:")
    # print(data_ids_accessed)

    # TODO: can record data paths instead of ids and move this to gatekeeper
    # user_id = pathlib.PurePath(args[-1]).parts[-2]
    # api_name = pathlib.PurePath(args[-1]).parts[-1]
    # data_ids_accessed = set()
    # for file_path in data_accessed:
    #     data_id = gatekeeper.record_data_ids_accessed(file_path, user_id, api_name)
    #     if data_id != None:
    #         data_ids_accessed.add(data_id)

    # with open("/tmp/data_accessed.txt", 'w') as f:
    #     for path in data_accessed:
    #         f.write(path + "\n")
    #     f.flush()
    #     os.fsync(f.fileno())
    send_end.send(list(data_accessed))

    # host = "localhost"
    # port = 6666
    # sock = socket.socket()
    # sock.bind((host, port))
    # sock.listen(1)
    # c, addr = sock.accept()
    # sock.send(str(data_ids_accessed).encode())
    # c.close()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], [])
