# Copyright (c) 2012 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import cPickle as pickle
import os
import xattr

from contextlib import contextmanager

from swift.common.utils import (normalize_timestamp, renamer)


METADATA_KEY = 'user.swift.metadata'
#STATUS_KEY = 'user.swift.status'
CONTCNT_KEY = 'user.swift.container_count'
PICKLE_PROTOCOL = 2
#MAX_XATTR_SIZE = 65536

# Using the same metadata protocol as the object server normally uses.
# XXX Gluster has a more elaborate verion with gradual unpickling. Why?
def read_metadata(path):
    metadata = ''
    key = 0
    try:
        while True:
            metadata += xattr.get(path, '%s%s' % (METADATA_KEY, (key or '')))
            key += 1
    except IOError:
        pass
    return pickle.loads(metadata)

def write_metadata(path, metadata):
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        xattr.set(path, '%s%s' % (METADATA_KEY, key or ''), metastr[:254])
        metastr = metastr[254:]
        key += 1


# XXX How about implementing a POSIX pbroker that does not use xattr?
class LFSPluginPosix():
    def __init__(self, app, account, container, obj):
        if obj:
            path = os.path.join(app.lfs_root, account, container, obj)
            self._type = 0 # like port 6010
        elif container:
            path = os.path.join(app.lfs_root, account, container)
            self._type = 1 # like port 6011
        else:
            path = os.path.join(app.lfs_root, account)
            self._type = 2 # like port 6012
        # P3
        fp = open("/tmp/dump","a")
        print >>fp, "posix path", path
        fp.close()
        self.datadir = path
        if os.path.exists(path):
            self.metadata = read_metadata(path)
        else:
            self.metadata = {}
        self.tmppath = None

    def exists(self):
        # The conventional Swift tries to distinguish between a valid account
        # and one that was initialized in the broker, doing complex checks
        # such as put_timestamp, delete_timestamp comparison. We omit that.
        # For now.
        return os.path.exists(self.datadir)

    def initialize(self, timestamp):
        # Junaid tries Exception and checkes for err.errno!=errno.EEXIST, but
        # in theory this should not be necessary if we implement the broker
        # protocol properly (Junaid's code has an empty initialize()).
        os.makedirs(self.datadir)
        #xattr.set(self.datadir, STATUS_KEY, 'OK')
        # Keeping stats counts in EA must be ridiculously inefficient. XXX
        if self._type == 2:
            xattr.set(self.datadir, CONTCNT_KEY, str(0))
        write_metadata(self.datadir, self.metadata)
        ts = int(float(timestamp))
        os.utime(self.datadir, (ts, ts))

    # All the status machinery is not intended in p-broker. Maybe never.
    #def is_status_deleted(self):
    #    # underlying account is marked as deleted
    #    status = xattr.get(self.datadir, STATUS_KEY)
    #    return status == 'DELETED'

    def get_info(self):
        name = os.path.basename(self.datadir)
        st = os.stat(self.datadir)
        if self._type == 2:
            cont_cnt_str = xattr.get(self.datadir, CONTCNT_KEY)
            try:
                container_count = int(cont_cnt_str)
            except ValueError:
                cont_cnt_str = "0"
            # XXX object_count, bytes_used
            return {'account': name,
                'created_at': normalize_timestamp(st.st_ctime),
                'put_timestamp': normalize_timestamp(st.st_mtime),
                'delete_timestamp': '0',
                'container_count': cont_cnt_str,
                'object_count': '0',
                'bytes_used': '0',
                'hash': '-',
                'id': ''}
        else:
            # XXX container info has something different in it, what is it?
            return {'container': name,
                'created_at': normalize_timestamp(st.st_ctime),
                'put_timestamp': normalize_timestamp(st.st_mtime),
                'delete_timestamp': '0',
                'object_count': '0',
                'bytes_used': '0',
                'hash': '-',
                'id': ''}

    # This is called a something_iter, but it is not actually an iterator.
    def list_containers_iter(self, limit,marker,end_marker,prefix,delimiter):
        # XXX implement marker, delimeter; consult CF devguide

        containers = os.listdir(self.datadir)
        containers.sort()
        containers.reverse()

        retults = []
        count = 0
        for cont in containers:
            if prefix:
                if not container.startswith(prefix):
                    continue
            if marker:
                if cont == marker:
                    marker = None
                continue
            if count < limit:
                # XXX (name, object_count, bytes_used, is_subdir)
                # XXX Should we encode in UTF-8 here or later?
                results.append([cont, 0, 0, 1])
                count += 1
        return results

    def update_metadata(self, metadata):
        if metadata:
            new_metadata = self.metadata.copy()
            new_metadata.update(metadata)
            if new_metadata != self.metadata:
                write_metadata(self.datadir, new_metadata)
                self.metadata = new_metadata

    def update_put_timestamp(self, timestamp):
        ts = int(float(timestamp))
        os.utime(self.datadir, (ts, ts))

    def put_container(self, container, put_timestamp, delete_timestamp,
                      object_count, bytes_used):
        assert(self._type == 2)
        # XXX Oops! This cannot be implemented without some kind of a database.
        # We need to find the record of a specific container; if it exists,
        # subtract its stats from account stats. Then, add new stats.
        #  -- look what Gluster people do and copy that, you wheel reinventor
        # XXX Don't forget locking or transactions.

        #cont_cnt_str = xattr.get(self.datadir, CONTCNT_KEY)
        #try:
        #    container_count = int(cont_cnt_str)
        #except ValueError:
        #    container_count = 0

    @contextmanager
    def mkstemp(self):
        """Contextmanager to make a temporary file."""

        # XXX Bad Zaitcev, no cookie. Create in tmpdir=self.lfs_root/tmp
        #if not os.path.exists(self.tmpdir):
        #    mkdirs(self.tmpdir)
        #fd, self.tmppath = mkstemp(dir=self.tmpdir)
        fd, self.tmppath = mkstemp(dir=self.datadir)
        try:
            yield fd
        finally:
            try:
                os.close(fd)
            except OSError:
                pass
            tmppath, self.tmppath = self.tmppath, None
            try:
                os.unlink(tmppath)
            except OSError:
                pass

    # In our case we don't actually use fd for anything.
    # XXX wanted? :param extension: extension to be used when making the file
    def put(self, fd, metadata):
        """
        Finalize writing the file on disk, and renames it from the temp file to
        the real location.  This should be called after the data has been
        written to the temp file.

        :param fd: file descriptor of the temp file
        :param metadata: dictionary of metadata to be written
        """
        assert self.tmppath is not None
        # wait, what?
        #metadata['name'] = self.name
        timestamp = normalize_timestamp(metadata['X-Timestamp'])
        write_metadata(self.tmppath, metadata)
        #if 'Content-Length' in metadata:
        #    self.drop_cache(fd, 0, int(metadata['Content-Length']))
        # XXX os.fsync maybe?
        #tpool.execute(fsync, fd)
        #renamer(self.tmppath,
        #        os.path.join(self.datadir, timestamp + extension))
        renamer(self.tmppath,
                os.path.join(self.datadir, timestamp))
        self.metadata = metadata
