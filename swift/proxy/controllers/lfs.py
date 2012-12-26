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

import os
import time
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


class AccControllerPosix(Controller):
    """WSGI controller for account requests"""
    server_type = 'Account'

    def __init__(self, app, account_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        # XXX needed?
        # if not self.app.allow_account_management:
        #     self.allowed_methods.remove('PUT')
        #     self.allowed_methods.remove('DELETE')

    @public
    def GET(self, req):
        """Handler for HTTP GET requests."""
        resp = HTTPBadRequest(request=req)
        resp.body = 'Not implemented'
        return resp

    @public
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        resp = HTTPBadRequest(request=req)
        resp.body = 'Not implemented'
        return resp

    # XXX later
    # @public
    # def POST(self, req):

    # For account management only
    # @public
    # def PUT(self, req):
    # @public
    # def DELETE(self, req):

class ContControllerPosix(Controller):
    """WSGI controller for container requests"""
    server_type = 'Container'

    def __init__(self, app, account_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)

    @public
    def GET(self, req):
        """Handler for HTTP GET requests."""
        resp = HTTPBadRequest(request=req)
        resp.body = 'Not implemented'
        return resp

    @public
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        resp = HTTPBadRequest(request=req)
        resp.body = 'Not implemented'
        return resp

    # XXX later
    # @public
    # def POST(self, req):

    # @public
    # def PUT(self, req):

    # @public
    # def DELETE(self, req):


class AccControllerGluster(Controller):
    """WSGI controller for account requests"""
    server_type = 'Account'

    def __init__(self, app, account_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        # XXX needed?
        # if not self.app.allow_account_management:
        #     self.allowed_methods.remove('PUT')
        #     self.allowed_methods.remove('DELETE')

        self.app = app

        # Config not available, assume check for /mnt/gluster-object
        #self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        # XXX XXX
        self.ufo_drive = "g"
        self.auto_create_account_prefix = "."

    @public
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        try:
            # XXX Container when accessing account? Is this ever possible (4)?
            v1, account = split_path(unquote(req.path), 2, 3)
            #validate_device_partition(drive, part)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        # XXX Wait, this can't be right. We mount on the root, right?
        #if not check_mount(self.app.lfs_root, drive):
        #    return HTTPInsufficientStorage(drive=drive, request=req)
        broker = DiskAccount(self.app.lfs_root, self.ufo_drive, account,
                             self.app.logger)
        if broker.is_deleted():
            # XXX Auto-create account
            resp = self._put(req, broker, account, None)
            if resp != HTTPCreated:
                return resp
            # XXX verify that this works without reopenning the broker
            if broker.is_deleted():
                return HTTPNotFound(request=req)
        info = broker.get_info()
        headers = {
            'X-Account-Container-Count': info['container_count'],
            'X-Account-Object-Count': info['object_count'],
            'X-Account-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp']}
        headers.update((key, value)
                       for key, (value, timestamp) in
                       broker.metadata.iteritems() if value != '')
        if get_param(req, 'format'):
            req.accept = FORMAT2CONTENT_TYPE.get(
                get_param(req, 'format').lower(), FORMAT2CONTENT_TYPE['plain'])
        headers['Content-Type'] = req.accept.best_match(
            ['text/plain', 'application/json', 'application/xml', 'text/xml'])
        if not headers['Content-Type']:
            return HTTPNotAcceptable(request=req)
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
        broker = DiskAccount(self.app.lfs_root, self.ufo_drive, account,
                 self.app.logger)
        # XXX why does it work to assign these variables to Gluster?
        broker.pending_timeout = 0.1
        broker.stale_reads_ok = True
        if broker.is_deleted():
            # XXX Auto-create account
            resp = self._put(req, broker, account, None)
            if resp != HTTPCreated:
                return resp
            # XXX verify that this works without reopenning the broker
            if broker.is_deleted():
                return HTTPNotFound(request=req)
        info = broker.get_info()
        resp_headers = {
            'X-Account-Container-Count': info['container_count'],
            'X-Account-Object-Count': info['object_count'],
            'X-Account-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp']}
        resp_headers.update((key, value)
                            for key, (value, timestamp) in
                            broker.metadata.iteritems() if value != '')
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
        account_list = broker.list_containers_iter(limit, marker, end_marker,
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
            v1, account, container = split_path(unquote(req.path), 2, 3)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        #if not check_mount(self.app.lfs_root, self.ufo_drive):
        #    return HTTPInsufficientStorage(request=req)
        broker = DiskAccount(self.app.lfs_root, self.ufo_drive, account,
                             self.app.logger)
        return self._put(req, broker, account, container)

    def _put(self, req, broker, account, container):
        if container:   # put account container
            if 'x-trans-id' in req.headers:
                broker.pending_timeout = 3
            if account.startswith(self.auto_create_account_prefix) and \
                    not os.path.exists(broker.db_file):
                broker.initialize(normalize_timestamp(
                    req.headers.get('x-timestamp') or time.time()))
            if req.headers.get('x-account-override-deleted', 'no').lower() != \
                    'yes' and broker.is_deleted():
                return HTTPNotFound(request=req)
            # XXX XXX We need these timestamps and counts
            #broker.put_container(container, req.headers['x-put-timestamp'],
            #                     req.headers['x-delete-timestamp'],
            #                     req.headers['x-object-count'],
            #                     req.headers['x-bytes-used'])
            #if req.headers['x-delete-timestamp'] > \
            #        req.headers['x-put-timestamp']:
            #    return HTTPNoContent(request=req)
            #else:
            #    return HTTPCreated(request=req)
            broker.put_container(container, time.time(), 0, 1, 111)
            return HTTPCreated(request=req)
        else:   # put account
            timestamp = normalize_timestamp(time.time())
            # P3
            fp = open("/tmp/dump","a")
            print >>fp, "db_file", broker.db_file
            fp.close()
            if not os.path.exists(broker.db_file):
                broker.initialize(timestamp)
                created = True
            elif broker.is_status_deleted():
                return HTTPForbidden(request=req, body='Recently deleted')
            else:
                created = broker.is_deleted()
                broker.update_put_timestamp(timestamp)
                if broker.is_deleted():
                    return HTTPConflict(request=req)
            metadata = {}
            metadata.update((key, (value, timestamp))
                            for key, value in req.headers.iteritems()
                            if key.lower().startswith('x-account-meta-'))
            if metadata:
                broker.update_metadata(metadata)
            if created:
                return HTTPCreated(request=req)
            else:
                return HTTPAccepted(request=req)

    # @public
    # def DELETE(self, req):


class ContControllerGluster(Controller):
    """WSGI controller for container requests"""
    server_type = 'Container'
    #save_headers = ['x-container-read', 'x-container-write',
    #                'x-container-sync-key', 'x-container-sync-to']
    save_headers = []

    def __init__(self, app, **kwargs):
        Controller.__init__(self, app)
        self.app = app

        # XXX XXX
        self.ufo_drive = "g"
        #self.auto_create_account_prefix = \
        #    app.conf.get('auto_create_account_prefix') or '.'
        self.auto_create_account_prefix = "."

    def account_update(self, req, account, container, broker):
        """
        Update the account server with latest container info.

        :param req: swob.Request object
        :param account: account name
        :param container: container name
        :param broker: container DB broker object
        :returns: if the account request returns a 404 error code,
                  HTTPNotFound response object, otherwise None.
        """
        info = broker.get_info()
        account_headers = {
            'x-put-timestamp': info['put_timestamp'],
            'x-delete-timestamp': info['delete_timestamp'],
            'x-object-count': info['object_count'],
            'x-bytes-used': info['bytes_used'],
            'x-trans-id': req.headers.get('x-trans-id', '-')}
        if req.headers.get('x-account-override-deleted', 'no').lower() == \
                'yes':
            account_headers['x-account-override-deleted'] = 'yes'
        # XXX deliver these account_headers into _put()
        abroker = DiskAccount(self.app.lfs_root, self.ufo_drive, account,
                         self.app.logger)
        d = dict(version='v1',
                 account_name=account,
                 container_name=container,
                 object_name=None)
        acont = AccControllerGluster(self.app, **d)
        return acont._put(req, abroker, account, container)

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
            v1, account, container, obj = split_path(unquote(req.path), 3, 4)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), content_type='text/plain',
                                  request=req)
        timestamp = normalize_timestamp(time.time())
        broker = DiskDir(self.app.lfs_root, self.ufo_drive, account, container,
                         self.app.logger)
        if obj:     # put container object
            if account.startswith(self.auto_create_account_prefix) and \
                    not os.path.exists(broker.db_file):
                broker.initialize(timestamp)
            if not os.path.exists(broker.db_file):
                return HTTPNotFound()
            # XXX Where to get the headers? Proxy inserts it somehow.
            #broker.put_object(obj, timestamp, int(req.headers['x-size']),
            #                  req.headers['x-content-type'],
            #                  req.headers['x-etag'])
            broker.put_object(obj, timestamp, None,None,None)
            return HTTPCreated(request=req)
        else:   # put container
            if not os.path.exists(broker.db_file):
                broker.initialize(timestamp)
                created = True
            else:
                created = broker.is_deleted()
                broker.update_put_timestamp(timestamp)
                if broker.is_deleted():
                    return HTTPConflict(request=req)
            metadata = {}
            metadata.update(
                (key, (value, timestamp))
                for key, value in req.headers.iteritems()
                if key.lower() in self.save_headers or
                key.lower().startswith('x-container-meta-'))
            if metadata:
                broker.update_metadata(metadata)
            resp = self.account_update(req, account, container, broker)
            if resp:
                return resp
            if created:
                return HTTPCreated(request=req)
            else:
                return HTTPAccepted(request=req)
