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

import errno
import os
import unittest
import xattr
from contextlib import contextmanager
from shutil import rmtree
from tempfile import mkdtemp

from eventlet import spawn, wsgi, listen

from test.unit import connect_tcp, readuntil2crlfs
from swift.proxy import server as proxy_server
from swift.common.utils import mkdirs, NullLogger

# XXX The xattr-patching code is stolen from test/unit/gluster/test_utls.py
# This is necessary so tests could be run in environments like Koji.
# Maybe we need to share this. XXX
from collections import defaultdict
#
# Somewhat hacky way of emulating the operation of xattr calls. They are made
# against a dictionary that stores the xattr key/value pairs.
#
_xattrs = {}
_xattr_op_cnt = defaultdict(int)
_xattr_err = {}

def _xkey(path, key):
    return "%s:%s" % (path, key)

def _setxattr(path, key, value):
    _xattr_op_cnt['set'] += 1
    xkey = _xkey(path, key)
    if xkey in _xattr_err:
        e = IOError()
        e.errno = _xattr_err[xkey]
        raise e
    global _xattrs
    _xattrs[xkey] = value

def _getxattr(path, key):
    _xattr_op_cnt['get'] += 1
    xkey = _xkey(path, key)
    if xkey in _xattr_err:
        e = IOError()
        e.errno = _xattr_err[xkey]
        raise e
    global _xattrs
    if xkey in _xattrs:
        ret_val = _xattrs[xkey]
    else:
        e = IOError("Fake IOError")
        e.errno = errno.ENODATA
        raise e
    return ret_val

def _removexattr(path, key):
    _xattr_op_cnt['remove'] += 1
    xkey = _xkey(path, key)
    if xkey in _xattr_err:
        e = IOError()
        e.errno = _xattr_err[xkey]
        raise e
    global _xattrs
    if xkey in _xattrs:
        del _xattrs[xkey]
    else:
        e = IOError("Fake IOError")
        e.errno = errno.ENODATA
        raise e

def _initxattr():
    global _xattrs
    _xattrs = {}
    global _xattr_op_cnt
    _xattr_op_cnt = defaultdict(int)
    global _xattr_err
    _xattr_err = {}

    # Save the current methods
    global _xattr_set;    _xattr_set    = xattr.set
    global _xattr_get;    _xattr_get    = xattr.get
    global _xattr_remove; _xattr_remove = xattr.remove

    # Monkey patch the calls we use with our internal unit test versions
    xattr.set    = _setxattr
    xattr.get    = _getxattr
    xattr.remove = _removexattr

def _destroyxattr():
    # Restore the current methods just in case
    global _xattr_set;    xattr.set    = _xattr_set
    global _xattr_get;    xattr.get    = _xattr_get
    global _xattr_remove; xattr.remove = _xattr_remove
    # Destroy the stored values and
    global _xattrs; _xattrs = None


def setup():
    _initxattr()

    global _testdir, _test_servers, _test_sockets, _test_coros
    _testdir = os.path.join(mkdtemp(), 'tmp_test_proxy_server_lfs')
    conf = {'devices': _testdir, 'swift_dir': _testdir,
            'mount_check': 'false', 'allow_versions': 'True',
            'allow_account_management': 'yes',
            'lfs_mode': 'gluster',
            'lfs_root': _testdir}
    mkdirs(_testdir)
    rmtree(_testdir)
    prolis = listen(('localhost', 0))
    _test_sockets = (prolis,)
    prosrv = proxy_server.Application(conf, FakeMemcacheReturnsNone(),
                                      None, FakeRing(), FakeRing(), FakeRing())
    _test_servers = (prosrv,)
    nl = NullLogger()
    prospa = spawn(wsgi.server, prolis, prosrv, nl)
    _test_coros = (prospa,)

    # Create account
    # XXX Why not create a controller directly and invoke it?
    # XXX Why not connect_tcp(prolis.getsockname())?
    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile()
    # P3
    #fd.write('HEAD /v1/a HTTP/1.1\r\nHost: localhost\r\n'
    #             'Connection: close\r\nContent-Length: 0\r\n\r\n')
    fd.write('GET /v1/a HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nContent-Length: 0\r\n\r\n')
    fd.flush()
    # P3
    #headers = readuntil2crlfs(fd)
    headers = fd.read()
    exp = 'HTTP/1.1 201'
    # P3
    fp = open("/tmp/dump","a")
    print >>fp, "== HEAD"
    print >>fp, headers
    fp.close()
    assert(headers[:len(exp)] == exp)

    # Create container
    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile()
    fd.write('PUT /v1/a/c HTTP/1.1\r\nHost: localhost\r\n'
             'Connection: close\r\nX-Auth-Token: t\r\n'
             'Content-Length: 0\r\n\r\n')
    fd.flush()
    # P3
    #headers = readuntil2crlfs(fd)
    headers = fd.read()
    exp = 'HTTP/1.1 201'
    # P3
    fp = open("/tmp/dump","a")
    print >>fp, "== PUT"
    print >>fp, headers
    fp.close()
    assert(headers[:len(exp)] == exp)

def teardown():
    for server in _test_coros:
        server.kill()
    rmtree(os.path.dirname(_testdir))
    _destroyxattr()


# XXX Get rid of the Ring eventually
class FakeRing(object):

    def __init__(self):
        # 9 total nodes (6 more past the initial 3) is the cap, no matter if
        # this is set higher.
        self.max_more_nodes = 0
        self.devs = {}

    def get_nodes(self, account, container=None, obj=None):
        devs = []
        for x in xrange(3):
            devs.append(self.devs.get(x))
            if devs[x] is None:
                self.devs[x] = devs[x] = \
                    {'ip': '10.0.0.%s' % x, 'port': 1000 + x, 'device': 'sda'}
        return 1, devs

    def get_part_nodes(self, part):
        return self.get_nodes('blah')[1]

    def get_more_nodes(self, nodes):
        # 9 is the true cap
        for x in xrange(3, min(3 + self.max_more_nodes, 9)):
            yield {'ip': '10.0.0.%s' % x, 'port': 1000 + x, 'device': 'sda'}


class FakeMemcache(object):

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def keys(self):
        return self.store.keys()

    def set(self, key, value, timeout=0):
        self.store[key] = value
        return True

    def incr(self, key, timeout=0):
        self.store[key] = self.store.setdefault(key, 0) + 1
        return self.store[key]

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except Exception:
            pass
        return True

class FakeMemcacheReturnsNone(FakeMemcache):

    def get(self, key):
        # Returns None as the timestamp of the container; assumes we're only
        # using the FakeMemcache for container existence checks.
        return None

class TestController(unittest.TestCase):

    #def setUp(self):
    #    _initxattr()

    #def tearDown(self):
    #    _destroyxattr()

    def test_nothing(self):
        print 'nothing'


if __name__ == '__main__':
    setup()
    try:
        unittest.main()
    finally:
        teardown()
