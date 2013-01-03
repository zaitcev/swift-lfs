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
import time
import xattr
from urllib import unquote

from swift.common.constraints import (ACCOUNT_LISTING_LIMIT, check_mount,
    FORMAT2CONTENT_TYPE)
from swift.common.swob import (HTTPAccepted,
    HTTPBadRequest,
    HTTPConflict,
    HTTPCreated,
    HTTPForbidden,
    HTTPInsufficientStorage,
    HTTPNoContent,
    HTTPNotAcceptable,
    HTTPNotFound,
    HTTPPreconditionFailed,
    Response)
from swift.common.utils import (get_logger, get_param, json,
    normalize_timestamp, public, split_path, validate_device_partition)
from swift.proxy.controllers.base import Controller

from gluster.swift.common.DiskDir import DiskDir, DiskAccount


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


# account_stat table contains
#    account - account name apparently, but why not "name"?
#    created_at - text, what is this?
#    put_timestamp - '0'
#    delete_timestamp - '0'
#    container_count
#    object_count
#    bytes_used
#    hash - of the above and name
#    id - huh?
#  + status (== 'DELETED')
#    status_changed_at - '0'
#  ? metadata - pickled? We save in xattr.

class LFSAccountController(Controller):
    """WSGI controller for account requests"""
    server_type = 'Account'

    def __init__(self, app, account_name, **kwargs):
        Controller.__init__(self, app)
        self.app = app

        #self.account_name = unquote(account_name)
        # XXX needed?
        # if not self.app.allow_account_management:
        #     self.allowed_methods.remove('PUT')
        #     self.allowed_methods.remove('DELETE')

        # Config not available, assume check for /mnt/gluster-object
        #self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        # XXX the config problem XXX
        self.auto_create_account_prefix = "."

    @public
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        try:
            v1, account = split_path(unquote(req.path), 2, 2)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        # XXX Wait, this can't be right. We mount on the root, right?
        #if not check_mount(self.app.lfs_root, drive):
        #    return HTTPInsufficientStorage(drive=drive, request=req)
        pbroker = _get_pbroker(self.app, account, None)
        created = False
        if not pbroker.exists():
            # XXX see, if only initialize() returned an error...
            #resp = pbroker.initialize(timestamp)
            #if resp != HTTPCreated:
            #    return resp
            pbroker.initialize(normalize_timestamp(time.time()))
            if not pbroker.exists():
                return HTTPNotFound(request=req)
            created = True
        info = pbroker.get_info()
        headers = {
            'X-Account-Container-Count': info['container_count'],
            'X-Account-Object-Count': info['object_count'],
            'X-Account-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp']}
        headers.update((key, value)
                       for key, (value, timestamp) in
                       pbroker.metadata.iteritems() if value != '')
        if get_param(req, 'format'):
            req.accept = FORMAT2CONTENT_TYPE.get(
                get_param(req, 'format').lower(), FORMAT2CONTENT_TYPE['plain'])
        headers['Content-Type'] = req.accept.best_match(
            ['text/plain', 'application/json', 'application/xml', 'text/xml'])
        if not headers['Content-Type']:
            return HTTPNotAcceptable(request=req)
        if created:
            return HTTPCreated(request=req, headers=headers, charset='utf-8')
        return HTTPNoContent(request=req, headers=headers, charset='utf-8')

    @public
    def GET(self, req):
        """Handler for HTTP GET requests."""
        try:
            v1, account = split_path(unquote(req.path), 2, 2)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        #if not check_mount(self.app.lfs_root, self.ufo_drive):
        #    return HTTPInsufficientStorage(request=req)
        pbroker = _get_pbroker(self.app, account, None)
        # XXX Why does it work to assign these variables to Gluster?
        # The DiskDir.py and friends do not seem to contain any code
        # to make this work or delegate to stock Swift.
        # We're going to skip this from plugin brocker for now.
        #broker.pending_timeout = 0.1
        #broker.stale_reads_ok = True
        if not pbroker.exists():
            pbroker.initialize(normalize_timestamp(time.time()))
            #resp = self._put(req, broker, account)
            #if resp != HTTPCreated:
            #    return resp
            if not pbroker.exists():
                return HTTPNotFound(request=req)
        info = pbroker.get_info()
        resp_headers = {
            'X-Account-Container-Count': info['container_count'],
            'X-Account-Object-Count': info['object_count'],
            'X-Account-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp']}
        resp_headers.update((key, value)
                            for key, (value, timestamp) in
                            pbroker.metadata.iteritems() if value != '')
        try:
            prefix = get_param(req, 'prefix')
            delimiter = get_param(req, 'delimiter')
            if delimiter and (len(delimiter) > 1 or ord(delimiter) > 254):
                # delimiters can be made more flexible later
                return HTTPPreconditionFailed(body='Bad delimiter')
            limit = ACCOUNT_LISTING_LIMIT
            given_limit = get_param(req, 'limit')
            if given_limit and given_limit.isdigit():
                limit = int(given_limit)
                if limit > ACCOUNT_LISTING_LIMIT:
                    return HTTPPreconditionFailed(request=req,
                                                  body='Maximum limit is %d' %
                                                  ACCOUNT_LISTING_LIMIT)
            marker = get_param(req, 'marker', '')
            end_marker = get_param(req, 'end_marker')
            query_format = get_param(req, 'format')
        except UnicodeDecodeError, err:
            return HTTPBadRequest(body='parameters not utf8',
                                  content_type='text/plain', request=req)
        if query_format:
            req.accept = FORMAT2CONTENT_TYPE.get(query_format.lower(),
                                                 FORMAT2CONTENT_TYPE['plain'])
        out_content_type = req.accept.best_match(
            ['text/plain', 'application/json', 'application/xml', 'text/xml'])
        if not out_content_type:
            return HTTPNotAcceptable(request=req)
        account_list = pbroker.list_containers_iter(limit, marker, end_marker,
                                                   prefix, delimiter)
        if out_content_type == 'application/json':
            data = []
            for (name, object_count, bytes_used, is_subdir) in account_list:
                if is_subdir:
                    data.append({'subdir': name})
                else:
                    data.append({'name': name, 'count': object_count,
                                'bytes': bytes_used})
            account_list = json.dumps(data)
        elif out_content_type.endswith('/xml'):
            output_list = ['<?xml version="1.0" encoding="UTF-8"?>',
                           '<account name="%s">' % account]
            for (name, object_count, bytes_used, is_subdir) in account_list:
                name = saxutils.escape(name)
                if is_subdir:
                    output_list.append('<subdir name="%s" />' % name)
                else:
                    item = '<container><name>%s</name><count>%s</count>' \
                           '<bytes>%s</bytes></container>' % \
                           (name, object_count, bytes_used)
                    output_list.append(item)
            output_list.append('</account>')
            account_list = '\n'.join(output_list)
        else:
            if not account_list:
                return HTTPNoContent(request=req, headers=resp_headers)
            account_list = '\n'.join(r[0] for r in account_list) + '\n'
        ret = Response(body=account_list, request=req, headers=resp_headers)
        ret.content_type = out_content_type
        ret.charset = 'utf-8'
        return ret

    # XXX later
    # @public
    # def POST(self, req):

    @public
    def PUT(self, req):
        """Handle HTTP PUT request."""
        try:
            # v1, account, container = split_path(unquote(req.path), 2, 3)
            v1, account = split_path(unquote(req.path), 2, 2)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        #if not check_mount(self.app.lfs_root, self.ufo_drive):
        #    return HTTPInsufficientStorage(request=req)
        pbroker = _get_pbroker(self.app, account, None)

        timestamp = normalize_timestamp(time.time())
        if not pbroker.exists():
            pbroker.initialize(timestamp)
            created = True
        #elif broker.is_status_deleted():
        #    return HTTPForbidden(request=req, body='Recently deleted')
        else:
            # XXX Figure out if thombstoning is in effect in LFS accounts ever.
            #created = broker.is_deleted()
            created = False
            pbroker.update_put_timestamp(timestamp)
            if not pbroker.exists():
                return HTTPConflict(request=req)
        metadata = {}
        metadata.update((key, (value, timestamp))
                        for key, value in req.headers.iteritems()
                        if key.lower().startswith('x-account-meta-'))
        if metadata:
            pbroker.update_metadata(metadata)
        if created:
            return HTTPCreated(request=req)
        else:
            return HTTPAccepted(request=req)

    # @public
    # def DELETE(self, req):

class LFSContainerController(Controller):
    """WSGI controller for container requests"""
    server_type = 'Container'
    #save_headers = ['x-container-read', 'x-container-write',
    #                'x-container-sync-key', 'x-container-sync-to']
    save_headers = []

    def __init__(self, app, **kwargs):
        Controller.__init__(self, app)
        self.app = app

        # XXX XXX
        #self.auto_create_account_prefix = \
        #    app.conf.get('auto_create_account_prefix') or '.'
        self.auto_create_account_prefix = "."

    # This is modelled at account_update() but is only called internally
    # in case of pbroker.
    def _account_update(self, req, account, container, pbroker):
        """
        Update the account server with latest container info.

        :param req: swob.Request object
        :param account: account name
        :param container: container name
        :param pbroker: container DB broker object
        :returns: if the account request returns a 404 error code,
                  HTTPNotFound response object, otherwise None.
        """
        info = pbroker.get_info()
        account_headers = {
            'x-put-timestamp': info['put_timestamp'],
            'x-delete-timestamp': info['delete_timestamp'],
            'x-object-count': info['object_count'],
            'x-bytes-used': info['bytes_used'],
            'x-trans-id': req.headers.get('x-trans-id', '-')}
        #if req.headers.get('x-account-override-deleted', 'no').lower() == \
        #        'yes':
        #    account_headers['x-account-override-deleted'] = 'yes'
        account_pbroker = _get_pbroker(self.app, account, None)
        #if account_headers.get('x-account-override-deleted', 'no').lower() != \
        #        'yes' and account_broker.is_deleted():
        #    return HTTPNotFound(request=req)
        account_pbroker.put_container(container,
                            account_headers['x-put-timestamp'],
                            account_headers['x-delete-timestamp'],
                            account_headers['x-object-count'],
                            account_headers['x-bytes-used'])
        if account_headers['x-delete-timestamp'] > \
                account_headers['x-put-timestamp']:
            return HTTPNoContent(request=req)
        return None

    @public
    def GET(self, req):
        """Handler for HTTP GET requests."""
        resp = HTTPBadRequest(request=req)
        resp.body = 'Not implemented'
        return resp

    @public
    def PUT(self, req):
        """Handle HTTP PUT request."""
        try:
            #v1, account, container, obj = split_path(unquote(req.path), 3, 4)
            v1, account, container = split_path(unquote(req.path), 3, 3)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        timestamp = normalize_timestamp(time.time())
        pbroker = _get_pbroker(self.app, account, container)

        #if obj:     # put container object
        #    if account.startswith(self.auto_create_account_prefix) and \
        #            not os.path.exists(broker.db_file):
        #        broker.initialize(timestamp)
        #    if not os.path.exists(broker.db_file):
        #        return HTTPNotFound()
        #    # XXX Where to get the headers? Proxy inserts it somehow.
        #    #broker.put_object(obj, timestamp, int(req.headers['x-size']),
        #    #                  req.headers['x-content-type'],
        #    #                  req.headers['x-etag'])
        #    broker.put_object(obj, timestamp, None,None,None)
        #    return HTTPCreated(request=req)
        #else:   # put container

        if not pbroker.exists():
            pbroker.initialize(timestamp)
            created = True
        else:
            # Why does Swift do the is_deleted() dange? Tombstoning?
            #created = broker.is_deleted()
            created = False
            pbroker.update_put_timestamp(timestamp)
            if not pbroker.exists():
                return HTTPConflict(request=req)
        metadata = {}
        metadata.update(
            (key, (value, timestamp))
            for key, value in req.headers.iteritems()
            if key.lower() in self.save_headers or
            key.lower().startswith('x-container-meta-'))
        if metadata:
            pbroker.update_metadata(metadata)
        resp = self._account_update(req, account, container, pbroker)
        if resp:
            return resp
        if created:
            return HTTPCreated(request=req)
        else:
            return HTTPAccepted(request=req)


def _get_pbroker(app, account, container):
    if app.lfs_mode == 'gluster':
        pbroker = LFSPluginGluster
    elif app.lfs_mode == 'posix':
        pbroker = LFSPluginPosix
    else:
        raise Exception("LFS unknown %s" % str(self.app.lfs_mode))
    return pbroker(app, account, container, None)


# XXX Not sure if it even makes sense to inherit if we get plugins loaded
# from files eventually. Maybe organize some kind of lfs_utils.py instead?
# XXX Calling this "plugin" doesn't feel right. Maybe "entity"?
# XXX What's up with the argument-driven a/c/o polymorphism, good or no good?
class LFSPlugin(object):
    # Methods that plugins implement (how to do abstract methods in Python?)
    #  __init__(self, app, account, container, obj)
    #  exists(self)
    #  initialize(self, timestamp) -- XXX return an error
    #  get_into(self)
    #  update_metadata(self, metadata)
    #  update_put_timestamp(self, timestamp)
    #  list_containers_iter(self, limit,marker,end_marker,prefix,delimiter)
    #  put_container(self, container, put_timestamp, delete_timestamp,
    #                obj_count, bytes_used)
    #  #put_object(self)
    # Properties that plugins implement
    #  metadata - read-only
    pass

# XXX How about implementing a POSIX pbroker that does not use xattr?
class LFSPluginPosix(LFSPlugin):
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

# For Gluster UFO we use a thin shim p-broker to traditional Swift broker,
# which is already implemented by Junaid and Peter. Hopefuly they'll just
# migrate to LFS later and then we drop this completely -- if we learn how
# to refer p-brokers in other modules etc. etc. etc.
class LFSPluginGluster(LFSPlugin):
    def __init__(self, app, account, container, obj):
        # XXX config from where? app something? XXX
        self.ufo_drive = "g"

        if obj:
            assert("not implemented yet")
        elif container:
            self.broker = DiskDir(app.lfs_root, self.ufo_drive, account,
                                  container, app.logger)
        else:
            self.broker = DiskAccount(app.lfs_root, self.ufo_drive, account,
                                      app.logger)
        # Ouch. This should work in case of read-only attribute though.
        self.metadata = self.broker.metadata

    def exists(self):
        # XXX verify that this works without reopenning the broker
        # Well, it should.... since initialize() is empty in Gluster.
        return not self.broker.is_deleted()

    def initialize(self, timestamp):
        # The method is empty in Gluster 3.3.x but that may change.
        self.broker.initialize(timestamp)

    def get_info(self):
        return self.broker.get_info()

    def update_metadata(self, metadata):
        return self.broker.update_metadata(metadata)

    def update_put_timestamp(self, timestamp):
        return self.broker.update_put_timestamp(metadata)

    def list_containers_iter(self, limit,marker,end_marker,prefix,delimiter):
        return self.broker.list_containers_iter(limit, marker, end_marker,
                                                prefix, delimiter)

    def put_container(self, container, put_timestamp, delete_timestamp,
                      object_count, bytes_used):
        # BTW, Gluster in 3.3.x does this:
        #   self.metadata[X_CONTAINER_COUNT] = (int(ccnt) + 1, put_timestamp)
        # Pays not attention to container. Discuss it with Peter XXX
        return self.broker.put_container(container,
            put_timestamp, delete_timestamp, object_count, bytes_used)
