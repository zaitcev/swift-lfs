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

# hopefuly we will get rid of import os with a better API to LFSPlugin
import os
import pkg_resources
import time
from hashlib import md5
from urllib import unquote

from eventlet import sleep
from eventlet.queue import Queue

from swift.common.constraints import (ACCOUNT_LISTING_LIMIT, check_mount,
    check_object_creation, FORMAT2CONTENT_TYPE, MAX_FILE_SIZE)
from swift.common.exceptions import (
    ChunkReadTimeout, ChunkWriteTimeout, ConnectionTimeout,
    DiskFileError, DiskFileNotExist,
    ListingIterNotFound, ListingIterNotAuthorized, ListingIterError)
from swift.common.swob import (
    HTTPAccepted,
    HTTPBadRequest,
    HTTPConflict,
    HTTPCreated,
    HTTPForbidden,
    HTTPInsufficientStorage,
    HTTPNoContent,
    HTTPNotAcceptable,
    HTTPNotFound,
    HTTPPreconditionFailed,
    HTTPRequestEntityTooLarge,
    Response)
from swift.common.utils import (ContextPool, get_param, json,
    normalize_timestamp, public)
from swift.proxy.controllers.base import Controller
from swift.proxy.controllers.obj import SegmentedIterable


import swift.proxy.lfs_posix

gluster_shortcut = True
try:
    import gluster.swift.common.lfs_plugin
except ImportError:
    gluster_shortcut = False

# raise Exception("LFS unknown %s" % str(self.app.lfs_mode))
def load_the_plugin(selector):
    # Special-case a local plugin - primarily for testing
    if selector == 'posix':
        return swift.proxy.lfs_posix.LFSPluginPosix
    # XXX
    if gluster_shortcut and selector == 'gluster':
        return gluster.swift.common.lfs_plugin.LFSPluginGluster
    group = 'swift.lfs_plugin.%s' % selector
    name = 'plugin_class'
    entry_point = pkg_resources.iter_entry_points(group, name=name)[0]
    plugin_class = entry_point.load()
    return plugin_class


# account_stat table contains
#    account - account name apparently, but why not "name"?
#    created_at - text, what is this?
#      XXX verify that we do fill out 'created_at' when creating
#      XXX document if 'created_at' ends a duty of plugins
#    put_timestamp - '0'
#    delete_timestamp - '0'
#    container_count
#    object_count
#    bytes_used
#    hash - of the above and name
#    id - huh?
#  + status (== 'DELETED')
#    status_changed_at - '0'
#  ? metadata - pickled? We save in a .meta file currently (or possibly xattr).
#      manifest - in x-object-manifest metadata for now, may promote later

class LFSAccountController(Controller):
    """WSGI controller for account requests"""
    server_type = 'Account'

    def __init__(self, app, account_name, **kwargs):
        Controller.__init__(self, app)
        #self.app = app
        self.account_name = unquote(account_name)

        # XXX needed?
        # if not self.app.allow_account_management:
        #     self.allowed_methods.remove('PUT')
        #     self.allowed_methods.remove('DELETE')

        # Config not available, assume check for /mnt/gluster-object
        # If we ever add this, we need to add check_mount into every method.
        #self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        # XXX the config problem XXX
        self.auto_create_account_prefix = "."

        # XXX this loads the class on every request
        self.plugin_class = load_the_plugin(self.app.lfs_mode)

    @public
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        # XXX Wait, this can't be right. We mount on the root, right?
        #if not check_mount(self.app.lfs_root, drive):
        #    return HTTPInsufficientStorage(drive=drive, request=req)
        pbroker = self.plugin_class(self.app, self.account_name,
                                    None, None, False)
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
        #if not check_mount(self.app.lfs_root, self.ufo_drive):
        #    return HTTPInsufficientStorage(request=req)
        pbroker = self.plugin_class(self.app, self.account_name,
                                    None, None, False)
        # XXX Why does it work to assign these variables to Gluster?
        # The DiskDir.py and friends do not seem to contain any code
        # to make this work or delegate to stock Swift.
        # We're going to skip this from plugin brocker for now.
        #broker.pending_timeout = 0.1
        #broker.stale_reads_ok = True
        if not pbroker.exists():
            pbroker.initialize(normalize_timestamp(time.time()))
            #resp = self._put(req, broker, self.account_name)
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
                           '<account name="%s">' % self.account_name]
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

    @public
    def PUT(self, req):
        """Handler for HTTP PUT request."""
        #if not check_mount(self.app.lfs_root, self.ufo_drive):
        #    return HTTPInsufficientStorage(request=req)
        pbroker = self.plugin_class(self.app, self.account_name,
                                    None, None, False)

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

        # What does this transfer actually do? Maybe just add the timestamp...
        headers = {'X-Timestamp': timestamp}
        self.transfer_headers(req.headers, headers)

        metadata = {}
        metadata.update((key, (value, timestamp))
                        for key, value in headers.iteritems()
                        if key.lower().startswith('x-account-meta-'))
        if metadata:
            pbroker.update_metadata(metadata)
        if created:
            return HTTPCreated(request=req)
        else:
            return HTTPAccepted(request=req)

    # POST to Account is only documented in CF Dev. Guide generally (7.7).
    # Is it supposed to return Created at all or only NoData always?
    @public
    def POST(self, req):
        """HTTP POST request handler."""
        #if not check_mount(self.app.lfs_root, self.ufo_drive):
        #    return HTTPInsufficientStorage(request=req)
        #error_response = check_metadata(req, 'account')
        #if error_response:
        #    return error_response
        timestamp = normalize_timestamp(time.time())
        pbroker = self.plugin_class(self.app, self.account_name,
                                    None, None, False)
        #if self.app.memcache:
        #    self.app.memcache.delete(
        #        get_account_memcache_key(self.account_name))
        # and self.app.account_autocreate:
        if not pbroker.exists():
            if len(self.account_name) > MAX_ACCOUNT_NAME_LENGTH:
                resp = HTTPBadRequest(request=req)
                resp.body = 'Account name length of %d longer than %d' % \
                            (len(self.account_name), MAX_ACCOUNT_NAME_LENGTH)
                return resp
            pbroker.initialize(timestamp)
        headers = {'X-Timestamp': timestamp}
        self.transfer_headers(req.headers, headers)
        metadata = {}
        metadata.update((key, (value, timestamp))
                        for key, value in headers.iteritems()
                        if key.lower().startswith('x-account-meta-'))
        if metadata:
            pbroker.update_metadata(metadata)
        return HTTPNoContent(request=req)

    # @public
    # def DELETE(self, req):

class LFSContainerController(Controller):
    """WSGI controller for container requests"""
    server_type = 'Container'
    #save_headers = ['x-container-read', 'x-container-write',
    #                'x-container-sync-key', 'x-container-sync-to']
    save_headers = []

    def __init__(self, app, account_name, container_name, **kwargs):
        Controller.__init__(self, app)
        #self.app = app
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)

        # XXX XXX
        #self.auto_create_account_prefix = \
        #    app.conf.get('auto_create_account_prefix') or '.'
        self.auto_create_account_prefix = "."

        # XXX this loads the class on every request
        self.plugin_class = load_the_plugin(self.app.lfs_mode)

    # clean_acls is taken verbatim from ContainerController
    def clean_acls(self, req):
        if 'swift.clean_acl' in req.environ:
            for header in ('x-container-read', 'x-container-write'):
                if header in req.headers:
                    try:
                        req.headers[header] = \
                            req.environ['swift.clean_acl'](header,
                                                           req.headers[header])
                    except ValueError, err:
                        return HTTPBadRequest(request=req, body=str(err))
        return None

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
        account_pbroker = self.plugin_class(self.app, account, None,None, False)
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

    #def _get_or_head_tail(self, req, resp):
    #    if 'swift.authorize' in req.environ:
    #        req.acl = resp.headers.get('x-container-read')
    #        aresp = req.environ['swift.authorize'](req)
    #        if aresp:
    #            return aresp
    #    if not req.environ.get('swift_owner', False):
    #        for key in ('x-container-read', 'x-container-write',
    #                    'x-container-sync-key', 'x-container-sync-to'):
    #            if key in resp.headers:
    #                del resp.headers[key]
    #    return resp

    @public
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        ## XXX account_info checks if account exists
        #if not self.account_info(self.account_name)[1]:
        #    return HTTPNotFound(request=req)

        #if self.mount_check and not check_mount(self.root, drive):
        #    return HTTPInsufficientStorage(drive=drive, request=req)

        pbroker = self.plugin_class(self.app,
                                    self.account_name, self.container_name,
                                    None, False)
        if not pbroker.exists():
            return HTTPNotFound(request=req)
        info = pbroker.get_info()
        headers = {
            'X-Container-Object-Count': info['object_count'],
            'X-Container-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp'],
        }
        headers.update(
            (key, value)
            for key, (value, timestamp) in pbroker.metadata.iteritems()
            if value != '' and (key.lower() in self.save_headers or
                                key.lower().startswith('x-container-meta-')))
        if get_param(req, 'format'):
            req.accept = FORMAT2CONTENT_TYPE.get(
                get_param(req, 'format').lower(), FORMAT2CONTENT_TYPE['plain'])
        headers['Content-Type'] = req.accept.best_match(
            ['text/plain', 'application/json', 'application/xml', 'text/xml'])
        if not headers['Content-Type']:
            return HTTPNotAcceptable(request=req)
        resp = HTTPNoContent(request=req, headers=headers, charset='utf-8')

        # this just does not seem to make sense
        #    update_headers(res, source.getheaders())
        #    if not res.environ:
        #        res.environ = {}
        #    res.environ['swift_x_timestamp'] = \
        #        source.getheader('x-timestamp')
        #    res.accept_ranges = 'bytes'
        #    res.content_length = source.getheader('Content-Length')
        #    if source.getheader('Content-Type'):
        #        res.charset = None
        #        res.content_type = source.getheader('Content-Type')

        #if self.app.memcache:
        #    # set the memcache container size for ratelimiting
        #    cache_key = get_container_memcache_key(self.account_name,
        #                                           self.container_name)
        #    self.app.memcache.set(
        #        cache_key,
        #        headers_to_container_info(resp.headers, resp.status_int),
        #        time=self.app.recheck_container_existence)

        # XXX Why does the original check the authorization _after_ the action?
        #resp = self._get_or_head_tail(req, resp)
        return resp

    @public
    def GET(self, req):
        """Handler for HTTP GET/HEAD requests."""
        ## XXX account_info checks if account exists
        #if not self.account_info(self.account_name)[1]:
        #    return HTTPNotFound(request=req)

        #if self.mount_check and not check_mount(self.root, drive):
        #    return HTTPInsufficientStorage(drive=drive, request=req)

        pbroker = self.plugin_class(self.app,
                                    self.account_name, self.container_name,
                                    None, False)
        if not pbroker.exists():
            return HTTPNotFound(request=req)
        info = pbroker.get_info()
        resp_headers = {
            'X-Container-Object-Count': info['object_count'],
            'X-Container-Bytes-Used': info['bytes_used'],
            'X-Timestamp': info['created_at'],
            'X-PUT-Timestamp': info['put_timestamp'],
        }
        resp_headers.update(
            (key, value)
            for key, (value, timestamp) in pbroker.metadata.iteritems()
            if value != '' and (key.lower() in self.save_headers or
                                key.lower().startswith('x-container-meta-')))
        try:
            path = get_param(req, 'path')
            prefix = get_param(req, 'prefix')
            delimiter = get_param(req, 'delimiter')
            if delimiter and (len(delimiter) > 1 or ord(delimiter) > 254):
                # delimiters can be made more flexible later
                return HTTPPreconditionFailed(body='Bad delimiter')
            marker = get_param(req, 'marker', '')
            end_marker = get_param(req, 'end_marker')
            limit = CONTAINER_LISTING_LIMIT
            given_limit = get_param(req, 'limit')
            if given_limit and given_limit.isdigit():
                limit = int(given_limit)
                if limit > CONTAINER_LISTING_LIMIT:
                    return HTTPPreconditionFailed(
                        request=req,
                        body='Maximum limit is %d' % CONTAINER_LISTING_LIMIT)
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
        container_list = pbroker.list_objects_iter(limit, marker, end_marker,
                                                   prefix, delimiter, path)
        if out_content_type == 'application/json':
            data = []
            for (name, created_at, size, content_type, etag) in container_list:
                if content_type is None:
                    data.append({"subdir": name})
                else:
                    created_at = datetime.utcfromtimestamp(
                        float(created_at)).isoformat()
                    # python isoformat() doesn't include msecs when zero
                    if len(created_at) < len("1970-01-01T00:00:00.000000"):
                        created_at += ".000000"
                    data.append({'last_modified': created_at, 'bytes': size,
                                'content_type': content_type, 'hash': etag,
                                'name': name})
            container_list = json.dumps(data)
        elif out_content_type.endswith('/xml'):
            xml_output = []
            for (name, created_at, size, content_type, etag) in container_list:
                # escape name and format date here
                name = saxutils.escape(name)
                created_at = datetime.utcfromtimestamp(
                    float(created_at)).isoformat()
                # python isoformat() doesn't include msecs when zero
                if len(created_at) < len("1970-01-01T00:00:00.000000"):
                    created_at += ".000000"
                if content_type is None:
                    xml_output.append('<subdir name="%s"><name>%s</name>'
                                      '</subdir>' % (name, name))
                else:
                    content_type = saxutils.escape(content_type)
                    xml_output.append(
                        '<object><name>%s</name><hash>%s</hash>'
                        '<bytes>%d</bytes><content_type>%s</content_type>'
                        '<last_modified>%s</last_modified></object>' %
                        (name, etag, size, content_type, created_at))
            container_list = ''.join([
                '<?xml version="1.0" encoding="UTF-8"?>\n',
                '<container name=%s>' % saxutils.quoteattr(container),
                ''.join(xml_output), '</container>'])
        else:
            if not container_list:
                return HTTPNoContent(request=req, headers=resp_headers)
            container_list = '\n'.join(r[0] for r in container_list) + '\n'
        resp = Response(body=container_list, request=req, headers=resp_headers)
        resp.content_type = out_content_type
        resp.charset = 'utf-8'

        # this just does not seem to make sense
        #    update_headers(res, source.getheaders())
        #    if not res.environ:
        #        res.environ = {}
        #    res.environ['swift_x_timestamp'] = \
        #        source.getheader('x-timestamp')
        #    res.accept_ranges = 'bytes'
        #    res.content_length = source.getheader('Content-Length')
        #    if source.getheader('Content-Type'):
        #        res.charset = None
        #        res.content_type = source.getheader('Content-Type')

        #if self.app.memcache:
        #    # set the memcache container size for ratelimiting
        #    cache_key = get_container_memcache_key(self.account_name,
        #                                           self.container_name)
        #    self.app.memcache.set(
        #        cache_key,
        #        headers_to_container_info(resp.headers, resp.status_int),
        #        time=self.app.recheck_container_existence)

        # XXX Why does the original check the authorization _after_ the action?
        #resp = self._get_or_head_tail(req, resp)
        return resp

    @public
    def PUT(self, req):
        """Handler for HTTP PUT request."""

        #account_partition, accounts, container_count = \
        #    self.account_info(self.account_name,
        #                      autocreate=self.app.account_autocreate)
        #if self.app.max_containers_per_account > 0 and \
        #        container_count >= self.app.max_containers_per_account and \
        #        self.account_name not in self.app.max_containers_whitelist:
        #    resp = HTTPForbidden(request=req)
        #    resp.body = 'Reached container limit of %s' % \
        #                self.app.max_containers_per_account
        #    return resp
        #if not accounts:
        #    return HTTPNotFound(request=req)

        timestamp = normalize_timestamp(time.time())
        pbroker = self.plugin_class(self.app,
                                    self.account_name, self.container_name,
                                    None, False)

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
        #if self.app.memcache:
        #    cache_key = get_container_memcache_key(self.account_name,
        #                                           self.container_name)
        #    self.app.memcache.delete(cache_key)
        metadata = {}
        metadata.update(
            (key, (value, timestamp))
            for key, value in req.headers.iteritems()
            if key.lower() in self.save_headers or
            key.lower().startswith('x-container-meta-'))
        if metadata:
            pbroker.update_metadata(metadata)
        resp = self._account_update(req, self.account_name, self.container_name,
                                    pbroker)
        if resp:
            return resp
        if created:
            return HTTPCreated(request=req)
        else:
            return HTTPAccepted(request=req)

    #@cors_validation
    @public
    def POST(self, req):
        """HTTP POST request handler."""
        #error_response = \
        #    self.clean_acls(req) or check_metadata(req, 'container')
        error_response = self.clean_acls(req)
        if error_response:
            return error_response
        timestamp =  normalize_timestamp(time.time())
        # XXX account_info verifies that account exists
        #account_partition, accounts, container_count = \
        #    self.account_info(self.account_name,
        #                      autocreate=self.app.account_autocreate)
        #if not accounts:
        #    return HTTPNotFound(request=req)
        pbroker = self.plugin_class(self.app,
                                    self.account_name, self.container_name,
                                    None, False)
        if not pbroker.exists():
            pbroker.initialize(timestamp)
            created = True
        #headers = {'X-Timestamp': normalize_timestamp(time.time()),
        #           'x-trans-id': self.trans_id,
        #           'Connection': 'close'}
        headers = {}
        self.transfer_headers(req.headers, headers)
        #if self.app.memcache:
        #    self.app.memcache.delete(get_container_memcache_key(
        #        self.account_name, self.container_name))

        #if 'x-container-sync-to' in headers:
        #    err = validate_sync_to(headers['x-container-sync-to'],
        #                           self.allowed_sync_hosts)
        #    if err:
        #        return HTTPBadRequest(err)

        #if self.mount_check and not check_mount(self.root, drive):
        #    return HTTPInsufficientStorage(drive=drive, request=req)

        metadata = {}
        metadata.update(
            (key, (value, timestamp)) for key, value in headers.iteritems()
            if key.lower() in self.save_headers or
            key.lower().startswith('x-container-meta-'))
        if metadata:
            #if 'X-Container-Sync-To' in metadata:
            #    if 'X-Container-Sync-To' not in pbroker.metadata or \
            #            metadata['X-Container-Sync-To'][0] != \
            #            pbroker.metadata['X-Container-Sync-To'][0]:
            #        pbroker.set_x_container_sync_points(-1, -1)
            pbroker.update_metadata(metadata)
        return HTTPNoContent(request=req)


class LFSObjectController(Controller):
    """WSGI controller for object requests."""
    server_type = 'Object'

    def __init__(self, app, account_name, container_name, object_name,
                 **kwargs):
        Controller.__init__(self, app)
        #self.app = app
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        self.object_name = unquote(object_name)

        #self.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.max_upload_time = 86400
        #self.bytes_per_sync = int(conf.get('mb_per_sync', 512)) * 1024 * 1024
        self.bytes_per_sync = 512 * 1024 * 1024

        self.allowed_headers = set([
            'content-disposition',
            'content-encoding',
            'x-delete-at',
            'x-object-manifest'])

        self.retained_meta_keys = set([
            'Content-Type',
            'Content-Length',
            'ETag'])

        # XXX this loads the class on every request
        self.plugin_class = load_the_plugin(self.app.lfs_mode)

    #def _listing_iter(self, lcontainer, lprefix, env):
    #    for page in self._listing_pages_iter(lcontainer, lprefix, env):
    #        for item in page:
    #            yield item

    #def _listing_pages_iter(self, lcontainer, lprefix, env):
    #    lpartition, lnodes = self.app.container_ring.get_nodes(
    #        self.account_name, lcontainer)
    #    marker = ''
    #    while True:
    #        lreq = Request.blank('i will be overridden by env', environ=env)
    #        # Don't quote PATH_INFO, by WSGI spec
    #        lreq.environ['PATH_INFO'] = \
    #            '/%s/%s' % (self.account_name, lcontainer)
    #        lreq.environ['REQUEST_METHOD'] = 'GET'
    #        lreq.environ['QUERY_STRING'] = \
    #            'format=json&prefix=%s&marker=%s' % (quote(lprefix),
    #                                                 quote(marker))
    #        shuffle(lnodes)
    #        lresp = self.GETorHEAD_base(
    #            lreq, _('Container'), lpartition, lnodes, lreq.path_info,
    #            len(lnodes))
    #        if 'swift.authorize' in env:
    #            lreq.acl = lresp.headers.get('x-container-read')
    #            aresp = env['swift.authorize'](lreq)
    #            if aresp:
    #                raise ListingIterNotAuthorized(aresp)
    #        if lresp.status_int == HTTP_NOT_FOUND:
    #            raise ListingIterNotFound()
    #        elif not is_success(lresp.status_int):
    #            raise ListingIterError()
    #        if not lresp.body:
    #            break
    #        sublisting = json.loads(lresp.body)
    #        if not sublisting:
    #            break
    #        marker = sublisting[-1]['name']
    #        yield sublisting

    #def _remaining_items(self, listing_iter):
    #    """
    #    Returns an item-by-item iterator for a page-by-page iterator
    #    of item listings.

    #    Swallows listing-related errors; this iterator is only used
    #    after we've already started streaming a response to the
    #    client, and so if we start getting errors from the container
    #    servers now, it's too late to send an error to the client, so
    #    we just quit looking for segments.
    #    """
    #    try:
    #        for page in listing_iter:
    #            for item in page:
    #                yield item
    #    except ListingIterNotFound:
    #        pass
    #    except ListingIterError:
    #        pass
    #    except ListingIterNotAuthorized:
    #        pass

    # Originally this was GET in object server, now transplanted
    def _get_or_head(self, request):

        pbroker = self.plugin_class(self.app, self.account_name,
                                    self.container_name, self.object_name,
                                    True)
        # XXX Expired - this needs to be processed correctly
        # if file.is_deleted() or file.is_expired():
        if not pbroker.exists():
            if request.headers.get('if-match') == '*':
                return HTTPPreconditionFailed(request=request)
            else:
                return HTTPNotFound(request=request)
        try:
            file_size = pbroker.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            pbroker.quarantine()
            return HTTPNotFound(request=request)

        if request.headers.get('if-match') not in (None, '*') and \
                pbroker.metadata['ETag'] not in request.if_match:
            pbroker.close()
            return HTTPPreconditionFailed(request=request)
        if request.headers.get('if-none-match') is not None:
            if pbroker.metadata['ETag'] in request.if_none_match:
                resp = HTTPNotModified(request=request)
                resp.etag = pbroker.metadata['ETag']
                pbroker.close()
                return resp

        try:
            if_unmodified_since = request.if_unmodified_since
        except (OverflowError, ValueError):
            # catches timestamps before the epoch
            return HTTPPreconditionFailed(request=request)
        if if_unmodified_since and \
                datetime.fromtimestamp(
                    float(pbroker.metadata['X-Timestamp']), UTC) > \
                if_unmodified_since:
            pbroker.close()
            return HTTPPreconditionFailed(request=request)
        try:
            if_modified_since = request.if_modified_since
        except (OverflowError, ValueError):
            # catches timestamps before the epoch
            return HTTPPreconditionFailed(request=request)
        if if_modified_since and \
                datetime.fromtimestamp(
                    float(pbroker.metadata['X-Timestamp']), UTC) < \
                if_modified_since:
            pbroker.close()
            return HTTPNotModified(request=request)

        if request.method == 'HEAD':
            # Original Swift does not need to set HTTPNoContent anywhere
            # on HEAD path to objects. How does it do it?
            response = Response(status=204,
                                request=request, conditional_response=True)
        else:
            response = Response(app_iter=pbroker,
                                request=request, conditional_response=True)
        response.headers['Content-Type'] = pbroker.metadata.get(
            'Content-Type', 'application/octet-stream')
        for key, value in pbroker.metadata.iteritems():
            if key.lower().startswith('x-object-meta-') or \
                    key.lower() in self.allowed_headers:
                response.headers[key] = value
        response.etag = pbroker.metadata['ETag']
        response.last_modified = float(pbroker.metadata['X-Timestamp'])
        response.content_length = file_size

        # Umm, what?
        #if response.content_length < self.keep_cache_size and \
        #        (self.keep_cache_private or
        #         ('X-Auth-Token' not in request.headers and
        #          'X-Storage-Token' not in request.headers)):
        #    pbroker.keep_cache = True

        if 'Content-Encoding' in pbroker.metadata:
            response.content_encoding = pbroker.metadata['Content-Encoding']
        response.headers['X-Timestamp'] = pbroker.metadata['X-Timestamp']

        return response

    def GETorHEAD(self, req):
        """Handler for HTTP GET or HEAD requests."""
        container_info = self._container_info(self.account_name,
                                             self.container_name)
        req.acl = container_info['read_acl']
        # XXX Investigate why we only authorize explicitly for obj, but not a,c.
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp

        # or maybe self._get() and self._head()
        resp = self._get_or_head(req)

        ## XXX This is verbatim what stock Swift does: calls Response as if
        ## it were the app, new Response is instantiated. The problem is that
        ## we rolled 2 code paths into 1: proxy and object. Now we have WSGI
        ## app called twice. GET is idempotent, but still we must fix this.
        #    resp = req.get_response(resp)

        # also there's this in GETorHEADbase:
        #    res.environ['swift_x_timestamp'] = \
        #        source.getheader('x-timestamp')
        #    res.accept_ranges = 'bytes'
        #    res.content_length = source.getheader('Content-Length')
        #    if source.getheader('Content-Type'):
        #        res.charset = None
        #        res.content_type = source.getheader('Content-Type')
        #    return res

        # XXX Manifest handling is ripe for a factoring (below is verbatim cc)
        if 'x-object-manifest' in resp.headers:
            lcontainer, lprefix = \
                resp.headers['x-object-manifest'].split('/', 1)
            lcontainer = unquote(lcontainer)
            lprefix = unquote(lprefix)
            try:
                pages_iter = iter(self._listing_pages_iter(lcontainer, lprefix,
                                                           req.environ))
                listing_page1 = pages_iter.next()
                listing = itertools.chain(listing_page1,
                                          self._remaining_items(pages_iter))
            except ListingIterNotFound:
                return HTTPNotFound(request=req)
            except ListingIterNotAuthorized, err:
                return err.aresp
            except ListingIterError:
                return HTTPServerError(request=req)
            except StopIteration:
                listing_page1 = listing = ()

            if len(listing_page1) >= CONTAINER_LISTING_LIMIT:
                resp = Response(headers=resp.headers, request=req,
                                conditional_response=True)
                if req.method == 'HEAD':
                    # These shenanigans are because swob translates the HEAD
                    # request into a swob EmptyResponse for the body, which
                    # has a len, which eventlet translates as needing a
                    # content-length header added. So we call the original
                    # swob resp for the headers but return an empty iterator
                    # for the body.

                    def head_response(environ, start_response):
                        resp(environ, start_response)
                        return iter([])

                    head_response.status_int = resp.status_int
                    return head_response
                else:
                    resp.app_iter = SegmentedIterable(
                        self, lcontainer, listing, resp)

            else:
                # For objects with a reasonable number of segments, we'll serve
                # them with a set content-length and computed etag.
                if listing:
                    listing = list(listing)
                    content_length = sum(o['bytes'] for o in listing)
                    last_modified = max(o['last_modified'] for o in listing)
                    last_modified = datetime(*map(int, re.split('[^\d]',
                                             last_modified)[:-1]))
                    etag = md5(
                        ''.join(o['hash'] for o in listing)).hexdigest()
                else:
                    content_length = 0
                    last_modified = resp.last_modified
                    etag = md5().hexdigest()
                resp = Response(headers=resp.headers, request=req,
                                conditional_response=True)
                resp.app_iter = SegmentedIterable(self, lcontainer, listing,
                                                  resp)
                resp.content_length = content_length
                resp.last_modified = last_modified
                resp.etag = etag
            resp.headers['accept-ranges'] = 'bytes'
            # In case of a manifest file of nonzero length, the
            # backend may have sent back a Content-Range header for
            # the manifest. It's wrong for the client, though.
            resp.content_range = None

        return resp

    #@public
    #@cors_validation
    #@delay_denial
    @public
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req)

    #@public
    #@cors_validation
    #@delay_denial
    @public
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req)

    # Almost same protocol as Controller.container_info(), but for LFS.
    # We do not return partition or nodes, as they make no sense in LFS.
    # XXX Maybe get rid of this altogether? Just find a place for ACLs.
    # XXX We need 'versions', too.
    def _container_info(self, account, container, account_autocreate=False):
        container_info = {'status': 0, 'read_acl': None,
                          'write_acl': None, 'sync_key': None,
                          'count': None, 'bytes': None,
                          'versions': None}
        return container_info

    # This is not much different from _send_file, but different enough.
    # Returns a Response() with error code and etag packed in, ready for use.
    # In normal Swift, this response is composed by Controller.best_response().
    def _put_pipe(self, pbroker, data_source, chunked, req):
        #outgoing_headers = self._backend_requests(
        #    req, len(nodes), container_partition, containers,
        #    delete_at_part, delete_at_nodes)

        #  pile.spawn(self._connect_put_node, node_iter, partition,
        #               req.path_info, nheaders, self.app.logger.thread_locals)

        #            conn = http_connect(
        #                node['ip'], node['port'], node['device'], part, 'PUT',
        #                path, headers)

        #if self.mount_check and not check_mount(self.devices, device):
        #    return HTTPInsufficientStorage(drive=device, request=req)
        error_response = check_object_creation(req, self.object_name)
        if error_response:
            return error_response
        new_delete_at = int(req.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=req,
                                  content_type='text/plain')
        orig_timestamp = pbroker.metadata.get('X-Timestamp')
        upload_expiration = time.time() + self.max_upload_time
        etag = md5()
        upload_size = 0
        last_sync = 0
        with pbroker.mkstemp() as fd:
            while True:
                with ChunkReadTimeout(self.app.client_timeout):
                    try:
                        chunk = next(data_source)
                    except StopIteration:
                        break
                upload_size += len(chunk)
                if upload_size > MAX_FILE_SIZE:
                    return HTTPRequestEntityTooLarge(request=req)
                if time.time() > upload_expiration:
                    self.logger.increment('PUT.timeouts')
                    return HTTPRequestTimeout(request=req)
                etag.update(chunk)
                while chunk:
                    written = os.write(fd, chunk)
                    chunk = chunk[written:]
                # For large files sync every 512MB (by default) written
                if upload_size - last_sync >= self.bytes_per_sync:
                    tpool.execute(fsync, fd)
                    drop_buffer_cache(fd, last_sync, upload_size - last_sync)
                    last_sync = upload_size
                sleep()
            if 'content-length' in req.headers and \
                    int(req.headers['content-length']) != upload_size:
                return HTTPClientDisconnect(request=req)
            etag = etag.hexdigest()
            if 'etag' in req.headers and \
                    req.headers['etag'].lower() != etag:
                return HTTPUnprocessableEntity(request=req)
            # Well, 'created_at' does not work: DiskFile has no get_info().
            # info = pbroker.get_info()
            # 'X-Timestamp': info['created_at'],
            metadata = {
                'X-Timestamp': req.headers['X-Timestamp'],
                'Content-Type': req.headers['content-type'],
                'ETag': etag,
                'Content-Length': str(upload_size),
            }
            metadata.update(val for val in req.headers.iteritems()
                            if val[0].lower().startswith('x-object-meta-') and
                            len(val[0]) > 14)
            for header_key in self.allowed_headers:
                if header_key in req.headers:
                    header_caps = header_key.title()
                    metadata[header_caps] = req.headers[header_key]
            # XXX Implement this.
            #old_delete_at = int(pbroker.metadata.get('X-Delete-At') or 0)
            #if old_delete_at != new_delete_at:
            #    if new_delete_at:
            #        self.delete_at_update(
            #            'PUT', new_delete_at, account, container, obj,
            #            req.headers, device)
            #    if old_delete_at:
            #        self.delete_at_update(
            #            'DELETE', old_delete_at, account, container, obj,
            #            req.headers, device)
            pbroker.put(fd, metadata)

        #except ChunkReadTimeout, err:
        #    self.app.logger.warn(
        #        _('ERROR Client read timeout (%ss)'), err.seconds)
        #    self.app.logger.increment('client_timeouts')
        #    return HTTPRequestTimeout(request=req)
        #except (Exception, Timeout):
        #    self.app.logger.exception(
        #        _('ERROR Exception causing client disconnect'))
        #    return HTTPClientDisconnect(request=req)
        #if req.content_length and bytes_transferred < req.content_length:
        #    req.client_disconnect = True
        #    self.app.logger.warn(
        #        _('Client disconnected without sending enough data'))
        #    self.app.logger.increment('client_disconnects')
        #    return HTTPClientDisconnect(request=req)

        pbroker.unlinkold(metadata['X-Timestamp'])
        # XXX Dude, implement this. So close!
        #if not orig_timestamp or \
        #        orig_timestamp < req.headers['x-timestamp']:
        #    self.container_update(
        #        'PUT', account, container, obj, req.headers,
        #        {'x-size': pbroker.metadata['Content-Length'],
        #         'x-content-type': pbroker.metadata['Content-Type'],
        #         'x-timestamp': pbroker.metadata['X-Timestamp'],
        #         'x-etag': pbroker.metadata['ETag'],
        #         'x-trans-id': req.headers.get('x-trans-id', '-')},
        #        device)
        resp = HTTPCreated(request=req, etag=etag)
        return resp

    #@public
    #@cors_validation
    #@delay_denial
    @public
    def PUT(self, req):
        """HTTP PUT request handler."""
        container_info = self._container_info(
            self.account_name, self.container_name,
            account_autocreate=self.app.account_autocreate)
        req.acl = container_info['write_acl']
        req.environ['swift_sync_key'] = container_info['sync_key']
        object_versions = container_info['versions']
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        # XXX This is a check if container exists. Needed?
        #containers = container_info['nodes']
        #if not containers:
        #    return HTTPNotFound(request=req)
        pbroker = self.plugin_class(self.app, self.account_name,
                                    self.container_name, self.object_name,
                                    False)

        if 'x-delete-after' in req.headers:
            try:
                x_delete_after = int(req.headers['x-delete-after'])
            except ValueError:
                    return HTTPBadRequest(request=req,
                                          content_type='text/plain',
                                          body='Non-integer X-Delete-After')
            req.headers['x-delete-at'] = '%d' % (time.time() + x_delete_after)
        if 'x-delete-at' in req.headers:
            try:
                x_delete_at = int(req.headers['x-delete-at'])
                if x_delete_at < time.time():
                    return HTTPBadRequest(
                        body='X-Delete-At in past', request=req,
                        content_type='text/plain')
            except ValueError:
                return HTTPBadRequest(request=req, content_type='text/plain',
                                      body='Non-integer X-Delete-At')
            delete_at_container = str(
                x_delete_at /
                self.app.expiring_objects_container_divisor *
                self.app.expiring_objects_container_divisor)
            # XXX Looks like the scheduled deletion time posted to special
            # account, where it's rescanned by something. LFS needs to abstract.
            #delete_at_part, delete_at_nodes = \
            #    self.app.container_ring.get_nodes(
            #        self.app.expiring_objects_account, delete_at_container)
            delete_at_part = delete_at_nodes = None
        else:
            delete_at_part = delete_at_nodes = None
        # do a HEAD request for container sync and checking object versions
        # XXX This is some dubious stuff we carried over for versioned objects
        if 'x-timestamp' in req.headers or \
                (object_versions and not
                 req.environ.get('swift_versioned_copy')):
            # headers={'X-Newest': 'True'},
            info = pbroker.get_info()

        timestamp = normalize_timestamp(time.time())
        req.headers['X-Timestamp'] = timestamp
        if not pbroker.exists():
            pbroker.initialize(timestamp)

        # Sometimes the 'content-type' header exists, but is set to None.
        content_type_manually_set = True
        if not req.headers.get('content-type'):
            guessed_type, _junk = mimetypes.guess_type(req.path_info)
            req.headers['Content-Type'] = guessed_type or \
                'application/octet-stream'
            content_type_manually_set = False
        error_response = check_object_creation(req, self.object_name)
        if error_response:
            return error_response
        if object_versions and not req.environ.get('swift_versioned_copy'):
            is_manifest = 'x-object-manifest' in req.headers or \
                          'x-object-manifest' in self.metadata
            if self.exists() and not is_manifest:
                # This is a version manifest and needs to be handled
                # differently. First copy the existing data to a new object,
                # then write the data from this request to the version manifest
                # object.
                lcontainer = object_versions.split('/')[0]
                prefix_len = '%03x' % len(self.object_name)
                lprefix = prefix_len + self.object_name + '/'
                ts = info['created_at']
                if not ts:
                    ts = info['put_timestamp']
                new_ts = normalize_timestamp(ts)
                vers_obj_name = lprefix + new_ts
                copy_headers = {
                    'Destination': '%s/%s' % (lcontainer, vers_obj_name)}
                copy_environ = {'REQUEST_METHOD': 'COPY',
                                'swift_versioned_copy': True
                                }
                copy_req = Request.blank(req.path_info, headers=copy_headers,
                                         environ=copy_environ)
                copy_resp = self.COPY(copy_req)
                if is_client_error(copy_resp.status_int):
                    # missing container or bad permissions
                    return HTTPPreconditionFailed(request=req)
                elif not is_success(copy_resp.status_int):
                    # could not copy the data, bail
                    return HTTPServiceUnavailable(request=req)
                # not needed to re-load p-broker, copy does not alter source
                #pbroker = self.plugin_class(self.app, self.account_name,
                #                 self.container_name, self.object_name, False)

        reader = req.environ['wsgi.input'].read
        data_source = iter(lambda: reader(self.app.client_chunk_size), '')
        source_header = req.headers.get('X-Copy-From')
        source_resp = None
        if source_header:
            source_header = unquote(source_header)
            acct = req.path_info.split('/', 2)[1]
            if isinstance(acct, unicode):
                acct = acct.encode('utf-8')
            if not source_header.startswith('/'):
                source_header = '/' + source_header
            source_header = '/' + acct + source_header
            try:
                src_container_name, src_obj_name = \
                    source_header.split('/', 3)[2:]
            except ValueError:
                return HTTPPreconditionFailed(
                    request=req,
                    body='X-Copy-From header must be of the form'
                         '<container name>/<object name>')
            source_req = req.copy_get()
            source_req.path_info = source_header
            source_req.headers['X-Newest'] = 'true'
            orig_obj_name = self.object_name
            orig_container_name = self.container_name
            self.object_name = src_obj_name
            self.container_name = src_container_name
            source_resp = self.GET(source_req)
            if source_resp.status_int >= HTTP_MULTIPLE_CHOICES:
                return source_resp
            self.object_name = orig_obj_name
            self.container_name = orig_container_name
            new_req = Request.blank(req.path_info,
                                    environ=req.environ, headers=req.headers)
            data_source = source_resp.app_iter
            new_req.content_length = source_resp.content_length
            if new_req.content_length is None:
                # This indicates a transfer-encoding: chunked source object,
                # which currently only happens because there are more than
                # CONTAINER_LISTING_LIMIT segments in a segmented object. In
                # this case, we're going to refuse to do the server-side copy.
                return HTTPRequestEntityTooLarge(request=req)
            new_req.etag = source_resp.etag
            # we no longer need the X-Copy-From header
            del new_req.headers['X-Copy-From']
            if not content_type_manually_set:
                new_req.headers['Content-Type'] = \
                    source_resp.headers['Content-Type']
            if not config_true_value(
                    new_req.headers.get('x-fresh-metadata', 'false')):
                for k, v in source_resp.headers.items():
                    if k.lower().startswith('x-object-meta-'):
                        new_req.headers[k] = v
                for k, v in req.headers.items():
                    if k.lower().startswith('x-object-meta-'):
                        new_req.headers[k] = v
            req = new_req

        chunked = req.headers.get('transfer-encoding')
        resp = self._put_pipe(pbroker, data_source, chunked, req)
        if source_header:
            resp.headers['X-Copied-From'] = quote(
                source_header.split('/', 2)[2])
            if 'last-modified' in source_resp.headers:
                resp.headers['X-Copied-From-Last-Modified'] = \
                    source_resp.headers['last-modified']
            for k, v in req.headers.items():
                if k.lower().startswith('x-object-meta-'):
                    resp.headers[k] = v
        resp.last_modified = float(req.headers['X-Timestamp'])
        return resp

    #@public
    #@cors_validation
    #@delay_denial
    @public
    def POST(self, req):
        """HTTP POST request handler."""
        if 'x-delete-after' in req.headers:
            try:
                x_delete_after = int(req.headers['x-delete-after'])
            except ValueError:
                return HTTPBadRequest(request=req,
                                      content_type='text/plain',
                                      body='Non-integer X-Delete-After')
            req.headers['x-delete-at'] = '%d' % (time.time() + x_delete_after)

        #if self.app.object_post_as_copy:
        #    req.method = 'PUT'
        #    req.path_info = '/%s/%s/%s' % (
        #        self.account_name, self.container_name, self.object_name)
        #    req.headers['Content-Length'] = 0
        #    req.headers['X-Copy-From'] = quote('/%s/%s' % (self.container_name,
        #                                       self.object_name))
        #    req.headers['X-Fresh-Metadata'] = 'true'
        #    req.environ['swift_versioned_copy'] = True
        #    resp = self.PUT(req)
        #    # Older editions returned 202 Accepted on object POSTs, so we'll
        #    # convert any 201 Created responses to that for compatibility with
        #    # picky clients.
        #    if resp.status_int != HTTP_CREATED:
        #        return resp
        #    return HTTPAccepted(request=req)

        #error_response = check_metadata(req, 'object')
        #if error_response:
        #    return error_response

        container_info = self._container_info(
            self.account_name, self.container_name,
            account_autocreate=self.app.account_autocreate)
        req.acl = container_info['write_acl']
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if 'x-delete-at' in req.headers:
            try:
                x_delete_at = int(req.headers['x-delete-at'])
                if x_delete_at < time.time():
                    return HTTPBadRequest(
                        body='X-Delete-At in past', request=req,
                        content_type='text/plain')
            except ValueError:
                return HTTPBadRequest(request=req,
                                      content_type='text/plain',
                                      body='Non-integer X-Delete-At')
            delete_at_container = str(
                x_delete_at /
                self.app.expiring_objects_container_divisor *
                self.app.expiring_objects_container_divisor)
            # Not in LFS.
            #delete_at_part, delete_at_nodes = \
            #    self.app.container_ring.get_nodes(
            #        self.app.expiring_objects_account, delete_at_container)
            delete_at_part = delete_at_nodes = None
        else:
            delete_at_part = delete_at_nodes = None
        req.headers['X-Timestamp'] = normalize_timestamp(time.time())

        new_delete_at = int(req.headers.get('X-Delete-At') or 0)
        if new_delete_at and new_delete_at < time.time():
            return HTTPBadRequest(body='X-Delete-At in past', request=req,
                                  content_type='text/plain')
        #if self.mount_check and not check_mount(self.devices, device):
        #    return HTTPInsufficientStorage(drive=device, request=req)
        pbroker = self.plugin_class(self.app, self.account_name,
                                    self.container_name, self.object_name,
                                    False)
        if not pbroker.exists():
            return HTTPNotFound(request=req)
        try:
            pbroker.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            pbroker.quarantine()
            return HTTPNotFound(request=req)
        metadata = {'X-Timestamp': req.headers['x-timestamp']}
        # XXX Swift object server does not inherit explicitly, yet works. How?
        for meta_key in self.retained_meta_keys:
            if meta_key in pbroker.metadata:
                metadata[meta_key] = pbroker.metadata[meta_key]
        metadata.update(val for val in req.headers.iteritems()
                        if val[0].lower().startswith('x-object-meta-'))
        for header_key in self.allowed_headers:
            if header_key in req.headers:
                header_caps = header_key.title()
                metadata[header_caps] = req.headers[header_key]
        # XXX Implement this.
        #old_delete_at = int(pbroker.metadata.get('X-Delete-At') or 0)
        #if old_delete_at != new_delete_at:
        #    if new_delete_at:
        #        self.delete_at_update('PUT', new_delete_at,
        #                              self.account_name, self.container_name,
        #                              self.object_name, req.headers, device)
        #    if old_delete_at:
        #        self.delete_at_update('DELETE', old_delete_at,
        #                              self.account_name, self.container_name,
        #                              self.object_name, req.headers, device)

        # 4.3.6. Update Object Metadata
        #    A POST request will delete all existing metadata added with
        #    a previous PUT/POST.
        # XXX Okay, here it puts the metatada whole. Fine. But elsewhere?
        # Audit uses of update_metadata, see if they meant put_metadata.
        pbroker.put_metadata(metadata)
        return HTTPAccepted(request=req)

    #API
    #@public
    #@cors_validation
    #@delay_denial
    #def DELETE(self, req):
    #    """HTTP DELETE request handler."""
    #    container_info = self._container_info(self.account_name,
    #                                         self.container_name)
    #    container_partition = container_info['partition']
    #    containers = container_info['nodes']
    #    req.acl = container_info['write_acl']
    #    req.environ['swift_sync_key'] = container_info['sync_key']
    #    object_versions = container_info['versions']
    #    if object_versions:
    #        # this is a version manifest and needs to be handled differently
    #        lcontainer = object_versions.split('/')[0]
    #        prefix_len = '%03x' % len(self.object_name)
    #        lprefix = prefix_len + self.object_name + '/'
    #        last_item = None
    #        try:
    #            for last_item in self._listing_iter(lcontainer, lprefix,
    #                                                req.environ):
    #                pass
    #        except ListingIterNotFound:
    #            # no worries, last_item is None
    #            pass
    #        except ListingIterNotAuthorized, err:
    #            return err.aresp
    #        except ListingIterError:
    #            return HTTPServerError(request=req)
    #        if last_item:
    #            # there are older versions so copy the previous version to the
    #            # current object and delete the previous version
    #            orig_container = self.container_name
    #            orig_obj = self.object_name
    #            self.container_name = lcontainer
    #            self.object_name = last_item['name']
    #            copy_path = '/' + self.account_name + '/' + \
    #                        self.container_name + '/' + self.object_name
    #            copy_headers = {'X-Newest': 'True',
    #                            'Destination': orig_container + '/' + orig_obj
    #                            }
    #            copy_environ = {'REQUEST_METHOD': 'COPY',
    #                            'swift_versioned_copy': True
    #                            }
    #            creq = Request.blank(copy_path, headers=copy_headers,
    #                                 environ=copy_environ)
    #            copy_resp = self.COPY(creq)
    #            if is_client_error(copy_resp.status_int):
    #                # some user error, maybe permissions
    #                return HTTPPreconditionFailed(request=req)
    #            elif not is_success(copy_resp.status_int):
    #                # could not copy the data, bail
    #                return HTTPServiceUnavailable(request=req)
    #            # reset these because the COPY changed them
    #            self.container_name = lcontainer
    #            self.object_name = last_item['name']
    #            new_del_req = Request.blank(copy_path, environ=req.environ)
    #            container_info = self._container_info(self.account_name,
    #                                                 self.container_name)
    #            container_partition = container_info['partition']
    #            containers = container_info['nodes']
    #            new_del_req.acl = container_info['write_acl']
    #            new_del_req.path_info = copy_path
    #            req = new_del_req
    #            # remove 'X-If-Delete-At', since it is not for the older copy
    #            if 'X-If-Delete-At' in req.headers:
    #                del req.headers['X-If-Delete-At']
    #    if 'swift.authorize' in req.environ:
    #        aresp = req.environ['swift.authorize'](req)
    #        if aresp:
    #            return aresp
    #    if not containers:
    #        return HTTPNotFound(request=req)
    #    partition, nodes = self.app.object_ring.get_nodes(
    #        self.account_name, self.container_name, self.object_name)
    #    # Used by container sync feature
    #    if 'x-timestamp' in req.headers:
    #        try:
    #            req.headers['X-Timestamp'] = \
    #                normalize_timestamp(float(req.headers['x-timestamp']))
    #        except ValueError:
    #            return HTTPBadRequest(
    #                request=req, content_type='text/plain',
    #                body='X-Timestamp should be a UNIX timestamp float value; '
    #                     'was %r' % req.headers['x-timestamp'])
    #    else:
    #        req.headers['X-Timestamp'] = normalize_timestamp(time.time())

    #    headers = self._backend_requests(
    #        req, len(nodes), container_partition, containers)
    #    resp = self.make_requests(req, self.app.object_ring,
    #                              partition, 'DELETE', req.path_info, headers)
    #    return resp

    #API
    #this is needed to PUT, BTW
    #@public
    #@cors_validation
    #@delay_denial
    #def COPY(self, req):
    #    """HTTP COPY request handler."""
    #    dest = req.headers.get('Destination')
    #    if not dest:
    #        return HTTPPreconditionFailed(request=req,
    #                                      body='Destination header required')
    #    dest = unquote(dest)
    #    if not dest.startswith('/'):
    #        dest = '/' + dest
    #    try:
    #        _junk, dest_container, dest_object = dest.split('/', 2)
    #    except ValueError:
    #        return HTTPPreconditionFailed(
    #            request=req,
    #            body='Destination header must be of the form '
    #                 '<container name>/<object name>')
    #    source = '/' + self.container_name + '/' + self.object_name
    #    self.container_name = dest_container
    #    self.object_name = dest_object
    #    # re-write the existing request as a PUT instead of creating a new one
    #    # since this one is already attached to the posthooklogger
    #    req.method = 'PUT'
    #    req.path_info = '/' + self.account_name + dest
    #    req.headers['Content-Length'] = 0
    #    req.headers['X-Copy-From'] = quote(source)
    #    del req.headers['Destination']
    #    return self.PUT(req)

    #... IMPLEMENTATION in object server
    #class ObjectController(object):
    #"""Implements the WSGI application for the Swift Object Server."""

    #tbd
    #def __init__(self, conf):
    #    """
    #    Creates a new WSGI application for the Swift Object Server. An
    #    example configuration is given at
    #    <source-dir>/etc/object-server.conf-sample or
    #    /etc/swift/object-server.conf-sample.
    #    """
    #    self.logger = get_logger(conf, log_route='object-server')
    #    self.devices = conf.get('devices', '/srv/node/')
    #    self.mount_check = config_true_value(conf.get('mount_check', 'true'))
    #    self.node_timeout = int(conf.get('node_timeout', 3))
    #    self.conn_timeout = float(conf.get('conn_timeout', 0.5))
    #    self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
    #    self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
    #    self.keep_cache_size = int(conf.get('keep_cache_size', 5242880))
    #    self.keep_cache_private = \
    #        config_true_value(conf.get('keep_cache_private', 'false'))
    #    self.max_upload_time = int(conf.get('max_upload_time', 86400))
    #    self.slow = int(conf.get('slow', 0))
    #    self.bytes_per_sync = int(conf.get('mb_per_sync', 512)) * 1024 * 1024
    #    default_allowed_headers = '''
    #        content-disposition,
    #        content-encoding,
    #        x-delete-at,
    #        x-object-manifest,
    #    '''
    #    self.allowed_headers = set(
    #        i.strip().lower() for i in
    #        conf.get('allowed_headers', default_allowed_headers).split(',')
    #        if i.strip() and i.strip().lower() not in DISALLOWED_HEADERS)
    #    self.expiring_objects_account = \
    #        (conf.get('auto_create_account_prefix') or '.') + \
    #        'expiring_objects'
    #    self.expiring_objects_container_divisor = \
    #        int(conf.get('expiring_objects_container_divisor') or 86400)

    #tbd - internal for implementation
    #def async_update(self, op, account, container, obj, host, partition,
    #                 contdevice, headers_out, objdevice):
    #    """
    #    Sends or saves an async update.

    #    :param op: operation performed (ex: 'PUT', or 'DELETE')
    #    :param account: account name for the object
    #    :param container: container name for the object
    #    :param obj: object name
    #    :param host: host that the container is on
    #    :param partition: partition that the container is on
    #    :param contdevice: device name that the container is on
    #    :param headers_out: dictionary of headers to send in the container
    #                        request
    #    :param objdevice: device name that the object is in
    #    """
    #    full_path = '/%s/%s/%s' % (account, container, obj)
    #    if all([host, partition, contdevice]):
    #        try:
    #            with ConnectionTimeout(self.conn_timeout):
    #                ip, port = host.rsplit(':', 1)
    #                conn = http_connect(ip, port, contdevice, partition, op,
    #                                    full_path, headers_out)
    #            with Timeout(self.node_timeout):
    #                response = conn.getresponse()
    #                response.read()
    #                if is_success(response.status):
    #                    return
    #                else:
    #                    self.logger.error(_(
    #                        'ERROR Container update failed '
    #                        '(saving for async update later): %(status)d '
    #                        'response from %(ip)s:%(port)s/%(dev)s'),
    #                        {'status': response.status, 'ip': ip, 'port': port,
    #                         'dev': contdevice})
    #        except (Exception, Timeout):
    #            self.logger.exception(_(
    #                'ERROR container update failed with '
    #                '%(ip)s:%(port)s/%(dev)s (saving for async update later)'),
    #                {'ip': ip, 'port': port, 'dev': contdevice})
    #    async_dir = os.path.join(self.devices, objdevice, ASYNCDIR)
    #    ohash = hash_path(account, container, obj)
    #    self.logger.increment('async_pendings')
    #    write_pickle(
    #        {'op': op, 'account': account, 'container': container,
    #         'obj': obj, 'headers': headers_out},
    #        os.path.join(async_dir, ohash[-3:], ohash + '-' +
    #                     normalize_timestamp(headers_out['x-timestamp'])),
    #        os.path.join(self.devices, objdevice, 'tmp'))

    #tbd
    #def container_update(self, op, account, container, obj, headers_in,
    #                     headers_out, objdevice):
    #    """
    #    Update the container when objects are updated.

    #    :param op: operation performed (ex: 'PUT', or 'DELETE')
    #    :param account: account name for the object
    #    :param container: container name for the object
    #    :param obj: object name
    #    :param headers_in: dictionary of headers from the original request
    #    :param headers_out: dictionary of headers to send in the container
    #                        request(s)
    #    :param objdevice: device name that the object is in
    #    """
    #    conthosts = [h.strip() for h in
    #                 headers_in.get('X-Container-Host', '').split(',')]
    #    contdevices = [d.strip() for d in
    #                   headers_in.get('X-Container-Device', '').split(',')]
    #    contpartition = headers_in.get('X-Container-Partition', '')

    #    if len(conthosts) != len(contdevices):
    #        # This shouldn't happen unless there's a bug in the proxy,
    #        # but if there is, we want to know about it.
    #        self.logger.error(_('ERROR Container update failed: different  '
    #                            'numbers of hosts and devices in request: '
    #                            '"%s" vs "%s"' %
    #                            (headers_in.get('X-Container-Host', ''),
    #                             headers_in.get('X-Container-Device', ''))))
    #        return

    #    if contpartition:
    #        updates = zip(conthosts, contdevices)
    #    else:
    #        updates = []

    #    for conthost, contdevice in updates:
    #        self.async_update(op, account, container, obj, conthost,
    #                          contpartition, contdevice, headers_out,
    #                          objdevice)

    #tbd
    #def delete_at_update(self, op, delete_at, account, container, obj,
    #                     headers_in, objdevice):
    #    """
    #    Update the expiring objects container when objects are updated.

    #    :param op: operation performed (ex: 'PUT', or 'DELETE')
    #    :param account: account name for the object
    #    :param container: container name for the object
    #    :param obj: object name
    #    :param headers_in: dictionary of headers from the original request
    #    :param objdevice: device name that the object is in
    #    """
    #    # Quick cap that will work from now until Sat Nov 20 17:46:39 2286
    #    # At that time, Swift will be so popular and pervasive I will have
    #    # created income for thousands of future programmers.
    #    delete_at = max(min(delete_at, 9999999999), 0)
    #    updates = [(None, None)]

    #    partition = None
    #    hosts = contdevices = [None]
    #    headers_out = {'x-timestamp': headers_in['x-timestamp'],
    #                   'x-trans-id': headers_in.get('x-trans-id', '-')}
    #    if op != 'DELETE':
    #        partition = headers_in.get('X-Delete-At-Partition', None)
    #        hosts = headers_in.get('X-Delete-At-Host', '')
    #        contdevices = headers_in.get('X-Delete-At-Device', '')
    #        updates = [upd for upd in
    #                   zip((h.strip() for h in hosts.split(',')),
    #                       (c.strip() for c in contdevices.split(',')))
    #                   if all(upd) and partition]
    #        if not updates:
    #            updates = [(None, None)]
    #        headers_out['x-size'] = '0'
    #        headers_out['x-content-type'] = 'text/plain'
    #        headers_out['x-etag'] = 'd41d8cd98f00b204e9800998ecf8427e'

    #    for host, contdevice in updates:
    #        self.async_update(
    #            op, self.expiring_objects_account,
    #            str(delete_at / self.expiring_objects_container_divisor *
    #                self.expiring_objects_container_divisor),
    #            '%s-%s/%s/%s' % (delete_at, account, container, obj),
    #            host, partition, contdevice, headers_out, objdevice)


    #@public
    #@timing_stats
    #def PUT(self, request):
    #    """Handle HTTP PUT requests for the Swift Object Server."""
    #    try:
    #        device, partition, account, container, obj = \
    #            split_path(unquote(request.path), 5, 5, True)
    #        validate_device_partition(device, partition)
    #    except ValueError, err:
    #        return HTTPBadRequest(body=str(err), request=request,
    #                              content_type='text/plain')
    #    if self.mount_check and not check_mount(self.devices, device):
    #        return HTTPInsufficientStorage(drive=device, request=request)
    #    if 'x-timestamp' not in request.headers or \
    #            not check_float(request.headers['x-timestamp']):
    #        return HTTPBadRequest(body='Missing timestamp', request=request,
    #                              content_type='text/plain')
    #    error_response = check_object_creation(request, obj)
    #    if error_response:
    #        return error_response
    #    new_delete_at = int(request.headers.get('X-Delete-At') or 0)
    #    if new_delete_at and new_delete_at < time.time():
    #        return HTTPBadRequest(body='X-Delete-At in past', request=request,
    #                              content_type='text/plain')
    #    file = DiskFile(self.devices, device, partition, account, container,
    #                    obj, self.logger, disk_chunk_size=self.disk_chunk_size)
    #    orig_timestamp = file.metadata.get('X-Timestamp')
    #    upload_expiration = time.time() + self.max_upload_time
    #    etag = md5()
    #    upload_size = 0
    #    last_sync = 0
    #    with file.mkstemp() as fd:
    #        try:
    #            fallocate(fd, int(request.headers.get('content-length', 0)))
    #        except OSError:
    #            return HTTPInsufficientStorage(drive=device, request=request)
    #        reader = request.environ['wsgi.input'].read
    #        for chunk in iter(lambda: reader(self.network_chunk_size), ''):
    #            upload_size += len(chunk)
    #            if time.time() > upload_expiration:
    #                self.logger.increment('PUT.timeouts')
    #                return HTTPRequestTimeout(request=request)
    #            etag.update(chunk)
    #            while chunk:
    #                written = os.write(fd, chunk)
    #                chunk = chunk[written:]
    #            # For large files sync every 512MB (by default) written
    #            if upload_size - last_sync >= self.bytes_per_sync:
    #                tpool.execute(fsync, fd)
    #                drop_buffer_cache(fd, last_sync, upload_size - last_sync)
    #                last_sync = upload_size
    #            sleep()

    #        if 'content-length' in request.headers and \
    #                int(request.headers['content-length']) != upload_size:
    #            return HTTPClientDisconnect(request=request)
    #        etag = etag.hexdigest()
    #        if 'etag' in request.headers and \
    #                request.headers['etag'].lower() != etag:
    #            return HTTPUnprocessableEntity(request=request)
    #        metadata = {
    #            'X-Timestamp': request.headers['x-timestamp'],
    #            'Content-Type': request.headers['content-type'],
    #            'ETag': etag,
    #            'Content-Length': str(upload_size),
    #        }
    #        metadata.update(val for val in request.headers.iteritems()
    #                        if val[0].lower().startswith('x-object-meta-') and
    #                        len(val[0]) > 14)
    #        for header_key in self.allowed_headers:
    #            if header_key in request.headers:
    #                header_caps = header_key.title()
    #                metadata[header_caps] = request.headers[header_key]
    #        old_delete_at = int(file.metadata.get('X-Delete-At') or 0)
    #        if old_delete_at != new_delete_at:
    #            if new_delete_at:
    #                self.delete_at_update(
    #                    'PUT', new_delete_at, account, container, obj,
    #                    request.headers, device)
    #            if old_delete_at:
    #                self.delete_at_update(
    #                    'DELETE', old_delete_at, account, container, obj,
    #                    request.headers, device)
    #        file.put(fd, metadata)
    #    file.unlinkold(metadata['X-Timestamp'])
    #    if not orig_timestamp or \
    #            orig_timestamp < request.headers['x-timestamp']:
    #        self.container_update(
    #            'PUT', account, container, obj, request.headers,
    #            {'x-size': file.metadata['Content-Length'],
    #             'x-content-type': file.metadata['Content-Type'],
    #             'x-timestamp': file.metadata['X-Timestamp'],
    #             'x-etag': file.metadata['ETag'],
    #             'x-trans-id': request.headers.get('x-trans-id', '-')},
    #            device)
    #    resp = HTTPCreated(request=request, etag=etag)
    #    return resp

    #tbd
    #@public
    #@timing_stats
    #def GET(self, request):
    #    """Handle HTTP GET requests for the Swift Object Server."""
    #    try:
    #        device, partition, account, container, obj = \
    #            split_path(unquote(request.path), 5, 5, True)
    #    except ValueError, err:
    #        return HTTPBadRequest(body=str(err), request=request,
    #                              content_type='text/plain')
    #    if self.mount_check and not check_mount(self.devices, device):
    #        return HTTPInsufficientStorage(drive=device, request=request)
    #    file = DiskFile(self.devices, device, partition, account, container,
    #                    obj, self.logger, keep_data_fp=True,
    #                    disk_chunk_size=self.disk_chunk_size,
    #                    iter_hook=sleep)
    #    if file.is_deleted() or file.is_expired():
    #        if request.headers.get('if-match') == '*':
    #            return HTTPPreconditionFailed(request=request)
    #        else:
    #            return HTTPNotFound(request=request)
    #    try:
    #        file_size = file.get_data_file_size()
    #    except (DiskFileError, DiskFileNotExist):
    #        file.quarantine()
    #        return HTTPNotFound(request=request)
    #    if request.headers.get('if-match') not in (None, '*') and \
    #            file.metadata['ETag'] not in request.if_match:
    #        file.close()
    #        return HTTPPreconditionFailed(request=request)
    #    if request.headers.get('if-none-match') is not None:
    #        if file.metadata['ETag'] in request.if_none_match:
    #            resp = HTTPNotModified(request=request)
    #            resp.etag = file.metadata['ETag']
    #            file.close()
    #            return resp
    #    try:
    #        if_unmodified_since = request.if_unmodified_since
    #    except (OverflowError, ValueError):
    #        # catches timestamps before the epoch
    #        return HTTPPreconditionFailed(request=request)
    #    if if_unmodified_since and \
    #            datetime.fromtimestamp(
    #                float(file.metadata['X-Timestamp']), UTC) > \
    #            if_unmodified_since:
    #        file.close()
    #        return HTTPPreconditionFailed(request=request)
    #    try:
    #        if_modified_since = request.if_modified_since
    #    except (OverflowError, ValueError):
    #        # catches timestamps before the epoch
    #        return HTTPPreconditionFailed(request=request)
    #    if if_modified_since and \
    #            datetime.fromtimestamp(
    #                float(file.metadata['X-Timestamp']), UTC) < \
    #            if_modified_since:
    #        file.close()
    #        return HTTPNotModified(request=request)
    #    response = Response(app_iter=file,
    #                        request=request, conditional_response=True)
    #    response.headers['Content-Type'] = file.metadata.get(
    #        'Content-Type', 'application/octet-stream')
    #    for key, value in file.metadata.iteritems():
    #        if key.lower().startswith('x-object-meta-') or \
    #                key.lower() in self.allowed_headers:
    #            response.headers[key] = value
    #    response.etag = file.metadata['ETag']
    #    response.last_modified = float(file.metadata['X-Timestamp'])
    #    response.content_length = file_size
    #    if response.content_length < self.keep_cache_size and \
    #            (self.keep_cache_private or
    #             ('X-Auth-Token' not in request.headers and
    #              'X-Storage-Token' not in request.headers)):
    #        file.keep_cache = True
    #    if 'Content-Encoding' in file.metadata:
    #        response.content_encoding = file.metadata['Content-Encoding']
    #    response.headers['X-Timestamp'] = file.metadata['X-Timestamp']
    #    return request.get_response(response)

    #tbd
    #@public
    #@timing_stats
    #def HEAD(self, request):
    #    """Handle HTTP HEAD requests for the Swift Object Server."""
    #    try:
    #        device, partition, account, container, obj = \
    #            split_path(unquote(request.path), 5, 5, True)
    #        validate_device_partition(device, partition)
    #    except ValueError, err:
    #        resp = HTTPBadRequest(request=request)
    #        resp.content_type = 'text/plain'
    #        resp.body = str(err)
    #        return resp
    #    if self.mount_check and not check_mount(self.devices, device):
    #        return HTTPInsufficientStorage(drive=device, request=request)
    #    file = DiskFile(self.devices, device, partition, account, container,
    #                    obj, self.logger, disk_chunk_size=self.disk_chunk_size)
    #    if file.is_deleted() or file.is_expired():
    #        return HTTPNotFound(request=request)
    #    try:
    #        file_size = file.get_data_file_size()
    #    except (DiskFileError, DiskFileNotExist):
    #        file.quarantine()
    #        return HTTPNotFound(request=request)
    #    response = Response(request=request, conditional_response=True)
    #    response.headers['Content-Type'] = file.metadata.get(
    #        'Content-Type', 'application/octet-stream')
    #    for key, value in file.metadata.iteritems():
    #        if key.lower().startswith('x-object-meta-') or \
    #                key.lower() in self.allowed_headers:
    #            response.headers[key] = value
    #    response.etag = file.metadata['ETag']
    #    response.last_modified = float(file.metadata['X-Timestamp'])
    #    # Needed for container sync feature
    #    response.headers['X-Timestamp'] = file.metadata['X-Timestamp']
    #    response.content_length = file_size
    #    if 'Content-Encoding' in file.metadata:
    #        response.content_encoding = file.metadata['Content-Encoding']
    #    return response

    #tbd
    #@public
    #@timing_stats
    #def DELETE(self, request):
    #    """Handle HTTP DELETE requests for the Swift Object Server."""
    #    try:
    #        device, partition, account, container, obj = \
    #            split_path(unquote(request.path), 5, 5, True)
    #        validate_device_partition(device, partition)
    #    except ValueError, e:
    #        return HTTPBadRequest(body=str(e), request=request,
    #                              content_type='text/plain')
    #    if 'x-timestamp' not in request.headers or \
    #            not check_float(request.headers['x-timestamp']):
    #        return HTTPBadRequest(body='Missing timestamp', request=request,
    #                              content_type='text/plain')
    #    if self.mount_check and not check_mount(self.devices, device):
    #        return HTTPInsufficientStorage(drive=device, request=request)
    #    response_class = HTTPNoContent
    #    file = DiskFile(self.devices, device, partition, account, container,
    #                    obj, self.logger, disk_chunk_size=self.disk_chunk_size)
    #    if 'x-if-delete-at' in request.headers and \
    #            int(request.headers['x-if-delete-at']) != \
    #            int(file.metadata.get('X-Delete-At') or 0):
    #        return HTTPPreconditionFailed(
    #            request=request,
    #            body='X-If-Delete-At and X-Delete-At do not match')
    #    orig_timestamp = file.metadata.get('X-Timestamp')
    #    if file.is_deleted() or file.is_expired():
    #        response_class = HTTPNotFound
    #    metadata = {
    #        'X-Timestamp': request.headers['X-Timestamp'], 'deleted': True,
    #    }
    #    old_delete_at = int(file.metadata.get('X-Delete-At') or 0)
    #    if old_delete_at:
    #        self.delete_at_update('DELETE', old_delete_at, account,
    #                              container, obj, request.headers, device)
    #    file.put_metadata(metadata, tombstone=True)
    #    file.unlinkold(metadata['X-Timestamp'])
    #    if not orig_timestamp or \
    #            orig_timestamp < request.headers['x-timestamp']:
    #        self.container_update(
    #            'DELETE', account, container, obj, request.headers,
    #            {'x-timestamp': metadata['X-Timestamp'],
    #             'x-trans-id': request.headers.get('x-trans-id', '-')},
    #            device)
    #    resp = response_class(request=request)
    #    return resp

# XXX Not sure if it even makes sense to inherit if we get plugins loaded
# from files eventually. Maybe organize some kind of lfs_utils.py instead?
# XXX Calling this "plugin" doesn't feel right. Maybe "entity"?
# XXX What's up with the argument-driven a/c/o polymorphism, good or no good?
class LFSPlugin(object):
    # Methods that plugins implement (how to do abstract methods in Python?)
    # Not every method is applicable to every plugin kind, e.g. delete_object
    # applies to containers only. Obvious, but mind it.
    #  __init__(self, app, account, container, obj, keep_data_fp)
    #    The app must be a Swift app, with app.conf, app.logger and the like.
    #    The keep_data_fp is needed because GET calls __iter__ that makes
    #    this assumption. Maybe replace with an arming call? XXX
    #  exists(self)
    #    Return True is object exists. Since tombstoning is an implementation,
    #    we do not have is_deleted().
    #  initialize(self, timestamp) -- XXX return an error if failed
    #    LFS calls this for objects as well as accounts and containers.
    #  get_into(self)
    #    Return the dict with the properties, same as legacy.
    #  #get_container_timestamp(self)
    #    This is something we postponed implementing, hoping that get_info()
    #    be sufficient. Used in HEAD request by legacy code.
    #  update_metadata(self, metadata_updates)
    #    The argument is a list of (key, (value, timestamp)).
    #  update_put_timestamp(self, timestamp)
    #    Note that timestamps are always strings, not floats or ints. It may
    #    be unpleasant for plugins, but helps Swift to form HTTP headers.
    #  list_containers_iter(self, limit,marker,end_marker,prefix,delimiter)
    #    As comments mention everywhere, this is not an iterator, just
    #    confusing name. Returns a full list of containers. XXX maybe rename
    #  put_container(self, container, put_timestamp, delete_timestamp,
    #                obj_count, bytes_used)
    #  #put_object(self, name, timestamp, size, content_type, etag, deleted=0)
    #  list_objects_iter(self, limit,marker,end_marker,prefix,delimiter,path)
    #  __iter__(self) - to be assigned to Request.app_iter
    #    It seems that swob essentially requires the supplied callable of GET
    #    to be a class, because it checks for hasattr('app_iter_ranges').
    #  close(self)
    #  #app_iter_ranges(self, ....)
    #  #delete_object(self, name, timestamp)
    #  mkstemp(self)
    #    Temporarily mimics the usual DiskFile.mkstemp(), returns descriptor
    #    XXX os.write() seems like an unnecessary visibility into plugin
    #  put(self, fd, metadata)
    #    Aping DiskFile again. May change API when we get rid of fd.
    #  put_metadata(self, metadata)
    #    Like put(), only not changing the body of the object.
    #  unlinkold(self, timestamp)
    #  get_data_file_size(self)
    #  quarantine(self)
    # Properties that plugins implement
    #  metadata
    #    The metadata property must be accessed for lookup only. We may
    #    enforce the it later with a @property.
    # TBD methods possibly for plugins to implement
    #  - expiration
    #  - segmentation and manifest
    pass
