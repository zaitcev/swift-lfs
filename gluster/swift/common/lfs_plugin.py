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

# For Gluster UFO we use a thin shim p-broker to traditional Swift broker,
# which is already implemented by Junaid and Peter. Hopefuly they'll just
# migrate to LFS later and then we drop this file completely.


from gluster.swift.common.DiskDir import DiskDir, DiskAccount


# Let's just duck-type for avoid circular loading issues.
#class LFSPluginGluster(lfs.LFSPlugin):
class LFSPluginGluster():
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
