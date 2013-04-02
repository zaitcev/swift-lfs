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
import errno
import os
import xattr

from contextlib import contextmanager
from hashlib import md5
from tempfile import mkstemp

# XXX get rid of exceptions or find a way to define them for LFS plugins
from swift.common.exceptions import (DiskFileError, DiskFileNotExist)
from swift.common.utils import (mkdirs, normalize_timestamp, renamer)

DISALLOWED_HEADERS = set('content-length content-type deleted etag'.split())
X_CONTENT_LENGTH = 'Content-Length'


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
    # Swift's normal Object server does not check for no metadata.
    # But in our case it's possible to get p-broker initialized first,
    # which implies trying to read metadata, before .initialize() is called.
    # Therefore, avoid EOFError exception.
    if len(metadata) == 0:
        return {}
    return pickle.loads(metadata)

def read_meta_file(path):
    with open(path, "r") as fp:
        metadata = fp.read()
    return pickle.loads(metadata)

def write_metadata(path, metadata):
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        xattr.set(path, '%s%s' % (METADATA_KEY, key or ''), metastr[:254])
        metastr = metastr[254:]
        key += 1

def write_meta_file(path, metadata):
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    with open(path, "w") as fp:
        fp.write(metastr)

# N.B. This can return an object type metadata if tombstone is found.
def load_meta_file(obj_path):
    files = sorted(os.listdir(obj_path), reverse=True)
    meta_file = None
    data_file = None
    for file in files:
        if file.endswith('.ts'):
            metadata = {'deleted': True}
            return metadata
        if file.endswith('.meta') and not meta_file:
            meta_file = os.path.join(self.datadir, file)
        if file.endswith('.data') and not data_file:
            data_file = os.path.join(self.datadir, file)
        if meta_file and self.data_file:
            break
    if not meta_file:
        return None
    return read_meta_file(meta_file)

# XXX How about implementing a POSIX pbroker that does not use xattr?
class LFSPluginPosix():
    def __init__(self, app, account, container, obj, keep_data_fp):
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
        print >>fp, "posix __init__ type", self._type, "path", path
        fp.close()
        self.datadir = path
        self.tmpdir = os.path.join(app.lfs_root, "tmp")
        self.tmppath = None
        self.data_file = None
        self.meta_file = None
        self.fp = None

        self.started_at_0 = False
        self.read_to_eof = False
        self.iter_etag = None
        self.disk_chunk_size = 128*1024
        self.metadata = {}

        if not os.path.exists(self.datadir):
            # P3
            fp = open("/tmp/dump","a")
            print >>fp, "posix __init__ not exist"
            fp.close()
            return

        if self._type == 1 or self._type == 2:
            self.metadata = read_metadata(self.datadir)
            # P3
            fp = open("/tmp/dump","a")
            print >>fp, "posix __init__ done"
            fp.close()
            return

        # XXX use a common load_meta_file() here - when P3's are gone
        files = sorted(os.listdir(self.datadir), reverse=True)
        for file in files:
            if file.endswith('.ts'):
                self.data_file = self.meta_file = None
                self.metadata = {'deleted': True}
                # P3
                fp = open("/tmp/dump","a")
                print >>fp, "posix __init__ deleted"
                fp.close()
                return
            if file.endswith('.meta') and not self.meta_file:
                self.meta_file = os.path.join(self.datadir, file)
            if file.endswith('.data') and not self.data_file:
                self.data_file = os.path.join(self.datadir, file)
            if self.meta_file and self.data_file:
                break
        if not self.data_file:
            # P3
            fp = open("/tmp/dump","a")
            print >>fp, "posix __init__ no data"
            fp.close()
            return
        if not self.meta_file:
            # P3
            fp = open("/tmp/dump","a")
            print >>fp, "posix __init__ no meta"
            fp.close()
            return

        self.metadata = read_meta_file(self.meta_file)
        # P3
        fp = open("/tmp/dump","a")
        print >>fp, "posix __init__ ETag", self.metadata.get('ETag')
        fp.close()

        if keep_data_fp:
            self.fp = open(self.data_file, 'rb')

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
        # P3
        fp = open("/tmp/dump","a")
        print >>fp, "posix initialize path", self.datadir, "ts", timestamp
        fp.close()
        os.makedirs(self.datadir)
        #xattr.set(self.datadir, STATUS_KEY, 'OK')
        # Keeping stats counts in EA must be ridiculously inefficient. XXX
        if self._type == 2:
            xattr.set(self.datadir, CONTCNT_KEY, str(0))
            write_metadata(self.datadir, self.metadata)
        elif self._type == 1:
            write_metadata(self.datadir, self.metadata)
        else:
            pass
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
        # XXX implement delimeter; consult CF devguide

        containers = os.listdir(self.datadir)
        containers.sort()
        containers.reverse()

        results = []
        count = 0
        for cont in containers:
            if prefix:
                if not cont.startswith(prefix):
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

    def list_objects_iter(self, limit, marker, end_marker, prefix, delim, path):
        # XXX implement delimeter; consult CF devguide

        objects = os.listdir(self.datadir)
        objects.sort()
        objects.reverse()

        results = []
        count = 0
        for obj in objects:
            if prefix:
                if not obj.startswith(prefix):
                    continue
            if marker:
                if obj == marker:
                    marker = None
                continue
            if count < limit:
                # XXX (name, object_count, bytes_used, is_subdir)
                # XXX Should we encode in UTF-8 here or later?
                results.append([cont, 0, 0, 1])

                list_item = []
                list_item.append(obj)
                obj_path = os.path.join(self.datadir, obj)
                # XXX aww brother this is gonna hurt... a lot (file read * N)
                # We're taking it in the pants even worse than Gluster, because
                # they just fetch the xattr and we must find the .meta file.
                metadata = load_meta_file(obj_path)
                if metadata:
                    list_item.append(metadata[X_TIMESTAMP])
                    list_item.append(int(metadata[X_CONTENT_LENGTH]))
                    list_item.append(metadata[X_CONTENT_TYPE])
                    list_item.append(metadata[X_ETAG])
                results.append(list_item)

                count += 1
        return results

    def update_metadata(self, metadata):
        if self._type != 0:
            if metadata:
                # Both old Swift and Gluster attempt to optimize out the
                # case of unchanged metadata. We'll do it later.
                md = self.metadata.copy()
                for key, value_timestamp in metadata.iteritems():
                    value, timestamp = value_timestamp
                    if key not in md or timestamp > md[key][1]:
                        md[key] = value_timestamp
                write_metadata(self.datadir, md)
                self.metadata = md

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

        if not os.path.exists(self.tmpdir):
            mkdirs(self.tmpdir)
        fd, self.tmppath = mkstemp(dir=self.tmpdir)
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
    # We do not have the "extension" parameter for LFS to allow for meta file.
    def put(self, fd, metadata):
        """
        Finalize writing the file on disk, and renames it from the temp file to
        the real location.  This should be called after the data has been
        written to the temp file.

        :param fd: file descriptor of the temp file
        :param metadata: dictionary of metadata to be written
        """
        assert self.tmppath is not None
        assert self._type == 0
        # wait, what?
        #metadata['name'] = self.name
        timestamp = normalize_timestamp(metadata['X-Timestamp'])
        base_path = os.path.join(self.datadir, timestamp)
        # P3
        fp = open("/tmp/dump","a")
        print >>fp, "posix put old", self.tmppath, "new", base_path
        fp.close()
        write_meta_file(base_path + '.meta', metadata)
        #if 'Content-Length' in metadata:
        #    self.drop_cache(fd, 0, int(metadata['Content-Length']))
        # XXX os.fsync maybe?
        #tpool.execute(fsync, fd)
        renamer(self.tmppath, base_path + ".data")
        # but not setting self.data_file here, is this right?
        self.metadata = metadata

    def put_metadata(self, metadata):
        assert self._type == 0
        if not self.meta_file:
            return
        # P3
        fp = open("/tmp/dump","a")
        print >>fp, "posix put_meta", self.meta_file
        fp.close()
        write_meta_file(self.meta_file, metadata)
        # XXX os.fsync maybe?
        self.metadata = metadata

    def _handle_close_quarantine(self):
        """Check if file needs to be quarantined"""
        try:
            self.get_data_file_size()
        except DiskFileError:
            self.quarantine()
            return
        except DiskFileNotExist:
            return

        if self.iter_etag and self.started_at_0 and self.read_to_eof and \
                'ETag' in self.metadata and \
                self.iter_etag.hexdigest() != self.metadata.get('ETag'):
            self.quarantine()
        return

    def __iter__(self):
        """Returns an iterator over the data file."""
        try:
            #dropped_cache = 0
            read = 0
            self.started_at_0 = False
            self.read_to_eof = False
            if self.fp.tell() == 0:
                self.started_at_0 = True
                self.iter_etag = md5()
            while True:
                chunk = self.fp.read(self.disk_chunk_size)
                if chunk:
                    if self.iter_etag:
                        self.iter_etag.update(chunk)
                    read += len(chunk)
                    #if read - dropped_cache > (1024 * 1024):
                    #    self.drop_cache(self.fp.fileno(), dropped_cache,
                    #                    read - dropped_cache)
                    #    dropped_cache = read
                    yield chunk
                else:
                    self.read_to_eof = True
                    #self.drop_cache(self.fp.fileno(), dropped_cache,
                    #                read - dropped_cache)
                    break
        finally:
        #    if not self.suppress_file_closing:
        #        self.close()
            pass

    def close(self, verify_file=True):
        """
        Close the file. Will handle quarantining file if necessary.

        :param verify_file: Defaults to True. If false, will not check
                            file to see if it needs quarantining.
        """
        if self.fp:
            try:
                if verify_file:
                    self._handle_close_quarantine()
            except (Exception, Timeout), e:
                self.logger.error(_(
                    'ERROR DiskFile %(data_file)s in '
                    '%(data_dir)s close failure: %(exc)s : %(stack)'),
                    {'exc': e, 'stack': ''.join(traceback.format_stack()),
                     'data_file': self.data_file, 'data_dir': self.datadir})
            finally:
                self.fp.close()
                self.fp = None

    def unlinkold(self, timestamp):
        """
        Remove any older versions of the object file.  Any file that has an
        older timestamp than timestamp will be deleted.

        :param timestamp: timestamp to compare with each file
        """
        assert self._type == 0
        if not self.metadata or self.metadata['X-Timestamp'] >= timestamp:
            return

        assert self.data_file, \
            "Have metadata, %r, but no data_file" % self.metadata

        # XXX Scan the datadir and actually delete all old versions per docstr
        do_unlink(self.data_file)
        do_unlink(self.meta_file)

        self.metadata = {}
        self.data_file = None
        self.meta_file = None

    def get_data_file_size(self):
        """
        Returns the os.path.getsize for the file.  Raises an exception if this
        file does not match the Content-Length stored in the metadata. Or if
        self.data_file does not exist.

        :returns: file size as an int
        :raises DiskFileError: on file size mismatch.
        :raises DiskFileNotExist: on file not existing (including deleted)
        """
        assert self._type == 0
        try:
            file_size = 0
            if self.data_file:
                file_size = os.path.getsize(self.data_file)
                if 'Content-Length' in self.metadata:
                    metadata_size = int(self.metadata['Content-Length'])
                    if file_size != metadata_size:
                        raise DiskFileError(
                            'Content-Length of %s does not match file size '
                            'of %s' % (metadata_size, file_size))
                return file_size
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise
        raise DiskFileNotExist('Data File does not exist.')

    def quarantine(self):
        """
        In the case that a file is corrupted, move it to a quarantined
        area to allow replication to fix it.

        :returns: if quarantine is successful, path to quarantined
                  directory otherwise None
        """
        #from swift.obj.replicator quarantine_renamer, get_hashes
        #if not (self.is_deleted() or self.quarantined_dir):
        #    self.quarantined_dir = quarantine_renamer(self.device_path,
        #                                              self.data_file)
        #    self.logger.increment('quarantines')
        #    return self.quarantined_dir

        # stub for now XXX
        # P3
        fp = open("/tmp/dump","a")
        print >>fp, "posix quarantine", self.data_file + ".quar"
        fp.close()
        renamer(self.data_file, self.data_file + ".quar")
