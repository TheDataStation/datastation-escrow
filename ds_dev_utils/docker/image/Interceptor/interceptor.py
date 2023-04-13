#!/usr/bin/env python

#    Copyright (C) 2001  Jeff Epler  <jepler@unpythonic.dhs.org>
#    Copyright (C) 2006  Csaba Henk  <csaba.henk@creo.hu>
#
#    This program can be distributed under the terms of the GNU LGPL.
#    See the file COPYING.
#

import os, sys
import pathlib
import math
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

    # Filter out paths that are not the parent paths of any of the accessible data or itself

    def readdir(self, path, offset):

        # print("Calling readdir...")

        pid = Fuse.GetContext(self)["pid"]
        # print("Readdir: pid is", pid)
        in_other_process = False
        # print(pid, main_process_id_global, os.getpid())
        if pid not in accessible_data_dict_global.keys():
            print("Readdir: In other process!")
            in_other_process = True
        else:
            accessible_data_paths = accessible_data_dict_global[pid][0]
            # print("Interceptor: the accessible data paths are", accessible_data_paths)

        # print("Interceptor: readdir", path)
        path_to_access = pathlib.Path("." + path).absolute()
        # print(str(path_to_access))
        for e in os.listdir("." + path):
            if in_other_process:
                yield fuse.Direntry(e)
            else:
                for acc_path in accessible_data_paths:
                    if path_to_access in pathlib.Path(acc_path).parents or str(path_to_access) == acc_path:
                        # print("Interceptor: yield")
                        yield fuse.Direntry(e)
                        break

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
        if not os.access("." + path, mode):
            # if mode == os.R_OK:
            #     print("Interceptor: can't read")
            return -EACCES

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
    def get_XmpFile(Xmp_self):

        class XmpFile(object):

            def __init__(self, path, flags, *mode):

                self.file_path = os.path.join(Path.cwd(), path[1:])
                self.file = os.fdopen(os.open("." + path, flags, *mode),
                                      flag2mode(flags))
                self.fd = self.file.fileno()

                if hasattr(os, 'pread'):
                    self.iolock = None
                else:
                    self.iolock = Lock()

                # zz: recording data accessed here
                # TODO: in zero trust mode, should we record all access, including those illegal access with
                #      the wrong key?
                fuse_context = Fuse.GetContext(Xmp_self)
                pid = fuse_context["pid"]
                # print("Interceptor: PID is", pid)

                # if pid in accessible_data_dict_global.keys():
                #     print("Interceptor: Opened " + self.file_path + " in " + flag2mode(flags) + " mode")
                #     print("Interceptor: pid:", pid)

                if pid not in data_accessed_dict_global.keys():
                    data_accessed_dict_global[pid] = set()
                    # print("Interceptor: accessed data dict is", data_accessed_dict_global)

                cur_set = data_accessed_dict_global[pid]
                cur_set.add(str(self.file_path))
                data_accessed_dict_global[pid] = cur_set
                print("Data accessed is", data_accessed_dict_global[pid])

                self.symmetric_key = None
                self.decrypted_bytes = None
                # check if the file access is from the running api inside data station
                if pid in accessible_data_dict_global.keys():

                    # get the accessible_data_key_dict, if the dict is not None,
                    #  then we know it's running in no trust mode.
                    accessible_data_key_dict = accessible_data_dict_global[pid][1]

                    if accessible_data_key_dict is not None:

                        # get the symmetric key of the current file if it's accessible by the current user accessing
                        if self.file_path in accessible_data_key_dict.keys():
                            # print("Getting the current key...")
                            self.symmetric_key = accessible_data_key_dict[self.file_path]
                            # print("Key is", self.symmetric_key)

                            # Decrypt the entire file here and use it as a cache.
                            # since multiple read/writes can happen to the file,
                            # we don't want decrypt it for every access
                            if self.iolock:
                                self.iolock.acquire()
                                try:
                                    encrypted_bytes = self.file.read()
                                    print("Decryption starting......")
                                    print(len(encrypted_bytes))
                                    self.decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                                        ciphertext=encrypted_bytes,
                                        key=self.symmetric_key)
                                    print("Decrypted bytes are:")
                                    print(self.decrypted_bytes[:100])
                                finally:
                                    self.iolock.release()
                            else:
                                start = time.perf_counter()

                                file_size = os.stat(self.file_path).st_size
                                print(f"File size is {file_size}")
                                chunk_size = 2000000000
                                num_reads = int(math.ceil(file_size / chunk_size))
                                print(f"Number of reads is {num_reads}")
                                bytes_read = 0
                                encrypted_bytes = bytes()
                                for i in range(num_reads):
                                    if i != num_reads - 1:
                                        cur_chunk_bytes = os.pread(self.fd, chunk_size, bytes_read)
                                        encrypted_bytes += cur_chunk_bytes
                                        bytes_read += chunk_size
                                        print(bytes_read)
                                    else:
                                        cur_chunk_bytes = os.pread(self.fd, file_size - bytes_read, bytes_read)
                                        encrypted_bytes += cur_chunk_bytes
                                        bytes_read += chunk_size
                                        print(bytes_read)
                                print("Decryption starting......")
                                print(len(encrypted_bytes))
                                print(type(encrypted_bytes))
                                self.decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                                    ciphertext=encrypted_bytes,
                                    key=self.symmetric_key)
                                print("Decrypted bytes are:")
                                print(self.decrypted_bytes[:100])
                                end = time.perf_counter()
                                print(f"{end-start} seconds for decryption")

            def read(self, length, offset):
                # print("Interceptor: I am reading " + str(self.file_path))

                if self.iolock:
                    self.iolock.acquire()
                    try:
                        if self.symmetric_key is not None:
                            # print(self.symmetric_key)
                            # encrypted_bytes = self.file.read()
                            # decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                            #     ciphertext=encrypted_bytes,
                            #     key=symmetric_key)
                            # TODO: what happens if decryption fails? For now just return an empty byte.
                            #  We can't return something that's larger than length (the size of buffer we are trying
                            #  to read)
                            if self.decrypted_bytes is not None:
                                if offset >= len(self.decrypted_bytes):
                                    return b''
                                else:
                                    content = self.decrypted_bytes[offset:offset + length]
                                    return content
                            else:
                                print("Interceptor: Cannot decrypt ", self.file_path)
                                return b''
                        else:
                            self.file.seek(offset)
                            return self.file.read(length)
                    finally:
                        self.iolock.release()
                else:
                    if self.symmetric_key is not None:
                        # print(self.symmetric_key)
                        # encrypted_bytes = os.pread(self.fd, os.stat(self.file_path).st_size, 0)
                        # decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                        #     ciphertext=encrypted_bytes,
                        #     key=symmetric_key)
                        if self.decrypted_bytes is not None:
                            # print(self.decrypted_bytes.decode())
                            # print(len(self.decrypted_bytes), offset, length)
                            if offset >= len(self.decrypted_bytes):
                                return b''
                            else:
                                content = self.decrypted_bytes[offset:offset + length]
                                return content
                        else:
                            print("Interceptor: Cannot decrypt ", self.file_path)
                            return b''
                    else:
                        return os.pread(self.fd, length, offset)

            def write(self, buf, offset):
                print("Interceptor: I am writing " + str(self.file_path))

                if self.iolock:
                    self.iolock.acquire()
                    try:
                        if self.symmetric_key is not None:
                            # encrypted_bytes = self.file.read()
                            # decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                            #     ciphertext=encrypted_bytes,
                            #     key=symmetric_key)
                            if self.decrypted_bytes == None:
                                print("Interceptor: Cannot decrypt ", self.file_path)
                                return 0

                            new_bytes = self.decrypted_bytes[:offset] + buf
                            if offset + len(buf) < len(self.decrypted_bytes):
                                new_bytes += self.decrypted_bytes[offset + len(buf):]

                            new_encrypted_bytes = cryptoutils.encrypt_data_with_symmetric_key(
                                data=new_bytes,
                                key=self.symmetric_key
                            )
                            if new_encrypted_bytes is not None:
                                self.file.truncate(0)
                                self.file.write(new_encrypted_bytes)
                                return len(buf)
                            else:
                                print("Interceptor: Cannot encrypt ", self.file_path)
                                return 0
                        else:
                            self.file.seek(offset)
                            self.file.write(buf)
                            return len(buf)
                    finally:
                        self.iolock.release()
                else:
                    if self.symmetric_key is not None:

                        if self.decrypted_bytes == None:
                            print("Interceptor: Cannot decrypt ", self.file_path)
                            return 0

                        new_bytes = self.decrypted_bytes[:offset] + buf

                        if offset + len(buf) < len(self.decrypted_bytes):
                            new_bytes += self.decrypted_bytes[offset + len(buf):]

                        new_encrypted_bytes = cryptoutils.encrypt_data_with_symmetric_key(
                            data=new_bytes,
                            key=self.symmetric_key
                        )
                        if new_encrypted_bytes is not None:
                            self.file.truncate(0)
                            os.pwrite(self.fd, new_encrypted_bytes, 0)
                            return len(buf)
                        else:
                            print("Interceptor: Cannot encrypt ", self.file_path)
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
                # print("Interceptor: fgetattr " + str(self.file_path))
                return os.fstat(self.fd)

            def ftruncate(self, trunc_len):
                # print("Interceptor: ftruncate " + str(self.file_path) + " with length " + str(trunc_len))

                if self.symmetric_key is not None:
                    # self.truncate = True
                    # self.truncate_len = trunc_len
                    if self.iolock:
                        self.iolock.acquire()
                        try:
                            # encrypted_bytes = self.file.read()
                            # decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                            #     ciphertext=encrypted_bytes,
                            #     key=symmetric_key)
                            if self.decrypted_bytes == None:
                                print("Interceptor: Cannot decrypt ", self.file_path)
                                return

                            if trunc_len > len(self.decrypted_bytes):
                                new_bytes = self.decrypted_bytes
                            else:
                                new_bytes = self.decrypted_bytes[:trunc_len]
                            new_encrypted_bytes = cryptoutils.encrypt_data_with_symmetric_key(
                                data=new_bytes,
                                key=self.symmetric_key
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
                        # encrypted_bytes = os.pread(self.fd, os.stat(self.file_path).st_size, 0)
                        # decrypted_bytes = cryptoutils.decrypt_data_with_symmetric_key(
                        #     ciphertext=encrypted_bytes,
                        #     key=symmetric_key)
                        if self.decrypted_bytes == None:
                            print("Interceptor: Cannot decrypt ", self.file_path)
                            return

                        if trunc_len > len(self.decrypted_bytes):
                            new_bytes = self.decrypted_bytes
                        else:
                            new_bytes = self.decrypted_bytes[:trunc_len]

                        new_encrypted_bytes = cryptoutils.encrypt_data_with_symmetric_key(
                            data=new_bytes,
                            key=self.symmetric_key
                        )

                        if new_encrypted_bytes is not None:
                            self.file.truncate()
                            num_bytes_wrote = os.pwrite(self.fd, new_encrypted_bytes, 0)
                            assert num_bytes_wrote == len(new_encrypted_bytes)

                            self.fsync(isfsyncfile=True)

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

        self.file_class = self.get_XmpFile()

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
