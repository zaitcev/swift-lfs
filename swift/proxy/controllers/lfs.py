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

import pkg_resources
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
from swift.common.utils import (get_param, json,
    normalize_timestamp, public, split_path)
from swift.proxy.controllers.base import Controller


# raise Exception("LFS unknown %s" % str(self.app.lfs_mode))
def load_the_plugin(selector):
    # Special-case a local plugin - primarily for testing
    if selector == 'posix':
        import swift.proxy.lfs_posix
        return swift.proxy.lfs_posix.LFSPluginPosix
    # XXX
    if selector == 'gluster':
        import gluster.swift.common.lfs_plugin
        return gluster.swift.common.lfs_plugin.LFSPluginGluster
    group = 'swift.lfs_plugin.%s' % selector
    name = 'plugin_class'
    entry_point = pkg_resources.iter_entry_points(group, name=name)[0]
    plugin_class = entry_point.load()
    return plugin_class



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

        # XXX this loads the class on every request
        self.plugin_class = load_the_plugin(self.app.lfs_mode)

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
        pbroker = self.plugin_class(self.app, account, None, None)
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
        pbroker = self.plugin_class(self.app, account, None, None)
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
        pbroker = self.plugin_class(self.app, account, None, None)

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

        # XXX this loads the class on every request
        self.plugin_class = load_the_plugin(self.app.lfs_mode)

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
        account_pbroker = self.plugin_class(self.app, account, None, None)
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
        pbroker = self.plugin_class(self.app, account, container, None)

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


# XXX Not sure if it even makes sense to inherit if we get plugins loaded
# from files eventually. Maybe organize some kind of lfs_utils.py instead?
# XXX Calling this "plugin" doesn't feel right. Maybe "entity"?
# XXX What's up with the argument-driven a/c/o polymorphism, good or no good?
class LFSPlugin(object):
    # Methods that plugins implement (how to do abstract methods in Python?)
    # Not every method is applicable to every plugin kind, e.g. delete_object
    # applies to containers only. Obvious, but mind it.
    #  __init__(self, app, account, container, obj)
    #    The app must be a Swift app, with app.conf, app.logger and the like.
    #  exists(self)
    #    Return True is object exists. Since tombstoning is an implementation,
    #    we do not have is_deleted().
    #  initialize(self, timestamp) -- XXX return an error if failed
    #  get_into(self)
    #    Return the dict with the properties, same as legacy.
    #  #get_container_timestamp(self)
    #    This is something we postponed implementing, hoping that get_info()
    #    be sufficient. Used in HEAD request by legacy code.
    #  update_metadata(self, metadata)
    #  update_put_timestamp(self, timestamp)
    #    Note that timestamps are always strings, not floats or ints. It may
    #    be unpleasant for plugins, but helps Swift to form HTTP headers.
    #  list_containers_iter(self, limit,marker,end_marker,prefix,delimiter)
    #    As comments mention everywhere, this is not an iterator, just
    #    confusing name. Returns a full list of containers. XXX maybe rename
    #  put_container(self, container, put_timestamp, delete_timestamp,
    #                obj_count, bytes_used)
    #  #put_object(self, name, timestamp, size, content_type, etag, deleted=0)
    #  #list_objects_iter(self, limit,marker,end_marker,prefix,delimiter,path)
    #  #__call__(self) - to be assigned to Request.app_iter
    #    It seems that swob essentially requires the supplied callable of GET
    #    to be a class, because it checks for hasattr('app_iter_ranges').
    #  #app_iter_ranges(self, ....)
    #  #delete_object(self, name, timestamp)
    # Properties that plugins implement
    #  metadata
    #    The metadata property must be accessed for lookup only. We may
    #    enforce the it later with a @property.
    # TBD methods possibly for plugins to implement
    #  - expiration
    #  - segmentation and manifest
    pass
