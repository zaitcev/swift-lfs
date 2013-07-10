=================
LFS: Existing API
=================

Internal Users in Code
----------------------

``-
DatabaseBroker:
  test/unit/common/test_db.py
  # not used by anyone directly, inherited by ContainerBroker, AccountBroker

ContainerBroker:
  swift/container/auditor.py:
    _one_audit_pass
      (path,dev,part)[] = audit_location_generator
      container_audit(path)
            broker = ContainerBroker(path); broker.get_info()
            # that's all folks - side effects do the job
  swift/container/server.py:
        self.replicator_rpc = ReplicatorRpc(self.root, DATADIR,
            ContainerBroker, self.mount_check, logger=self.logger)
      ContainerController.REPLICATE
        replicator_rpc.dispatch() # the only use of ReplicatorRpc, gropes path
      ContainerController._get_container_broker
        return ContainerBroker(db_path, account=account, container=container,
                               logger=self.logger)
      ContainerController.DELETE
        broker = self._get_container_broker(drive, part, account, container)
        broker.db_file | broker.is_good()
        broker.initialize()
        broker.delete_object() # delete via container controller?
        broker.empty()
        broker.is_deleted()
        broker.delete_db()
      ContainerController.PUT
        broker = self._get_container_broker(drive, part, account, container)
        broker.update_metadata()
      ContainerController.HEAD
        broker = self._get_container_broker(drive, part, account, container)
        broker.pending_timeout = 0.1
        broker.stale_reads_ok = True
      ContainerController.GET
        broker = self._get_container_broker(drive, part, account, container)
        container_list = broker.list_objects_iter(limit, marker, end_marker,...)
      ContainerController.POST
        broker = self._get_container_broker(drive, part, account, container)
  swift/container/sync.py:
            broker = ContainerBroker(path)
                while time() < stop_at and sync_point2 < sync_point1:
                    rows = broker.get_items_since(sync_point2, 1)
                    broker.set_x_container_sync_points(None, sync_point2)
                if next_sync_point:
                    broker.set_x_container_sync_points(None, next_sync_point)
                    broker.set_x_container_sync_points(sync_point1, None)
                while time() < stop_at:
                    rows = broker.get_items_since(sync_point1, 1)
  swift/container/replicator.py:
    class ContainerReplicator(db_replicator.Replicator):
    self.brokerclass = db.ContainerBroker # see swift/account/replicator.py
  swift/container/updater.py:
    def process_container(self, dbfile):
      spawn(container_report(http_connect('PUT')))
      broker.reported()

  test/unit/common/test_db.py:
        # a number of invocations, using ":memory:" as path
        broker = ContainerBroker(':memory:', account='a', container='c')
        # monkey-patch
        # oddly, there aren't any tests - actual testing done in setUp?
        ContainerBroker.create_container_stat_table = \
            premetadata_create_container_stat_table

  test/unit/container/test_sync.py:
    creates a fake broker only

  test/unit/container/test_updater.py:
    # Has a basic test with cb.initialize(), cb.put_object(), etc.
    # The test has ContainerUpdater interact with ContainerBroker through
    # filesystem. See test_run_once().

  test/unit/common/test_db_replicator.py:
    db_replicator.Replicator.brokerclass  # see swift/account/replicator.py

AccountBroker:
  swift/account/auditor.py:
            broker = AccountBroker(path)
            if not broker.is_deleted():
                broker.get_info()
            # That's all, folks! Side effects do the work.

  swift/account/reaper.py:
    def reap_device(self, device):
      # "Called once per pass for each device on the server."
      # listdir, listdir, listdir -- but not audit_location_generator?
      broker = AccountBroker(os.path.join(hsh_path, fname))
      if broker.is_status_deleted() and not broker.empty():
        containers = list(broker.list_containers_iter(1000, marker, ... None))

  swift/account/server.py:
    self.replicator_rpc = ReplicatorRpc(self.root, DATADIR, AccountBroker, ...)
    def _get_account_broker(self, drive, part, account):
      return AccountBroker(db_path, account=account, logger=self.logger)
    AccountController.REPLICATE
      replicator_rpc.dispatch() # the only use of ReplicatorRpc
    AccountController.DELETE
      broker = self._get_account_broker(drive, part, account)
      if broker.is_deleted(): return
      broker.delete_db(req.headers['x-timestamp'])
    AccountController.PUT
      broker = self._get_account_broker(drive, part, account)
      broker.db_file | broker.is_good
      broker.initialize(normalize_timestamp(....))
      broker.put_container(container, req.headers['x-put-timestamp'], ...)
      broker.is_status_deleted()  # Not the same as broker.is_deleted
      broker.update_put_timestamp(timestamp)
      if broker.is_deleted(): return
      broker.update_metadata(metadata)
    AccountController.HEAD
      broker.pending_timeout = 0.1
      if broker.is_deleted(): return
      info = broker.get_info()
      broker.metadata.iteritems()
    AccountController.GET
      if broker.is_status_deleted():
        _deleted_response()
          if broker.is_status_deleted(): # redundant checking
      account_listing_response(account, req, out_content_type, broker, ...)
    AccountController.POST
      broker.update_metadata(metadata)

  swift/account/replicator.py:
    class AccountReplicator(db_replicator.Replicator):
      self.brokerclass = db.AccountBroker
      run_forever() # via bin/swift-account-replicator -> run_daemon()
        run_once()
          dirs += os.path.join(self.root, node['device'], self.datadir)
          walk_datadir(datadir, node_id)
          _replicate_object(self, partition, object_file, node_id)
            broker = self.brokerclass(object_file, pending_timeout=30)
            broker.reclaim()
            info = broker.get_replication_info() # info['count'] etc.
            broker.get_info()
            maybe quarantine_db(broker._db_file, broker.db_type)
            _repl_to_node(node, broker, partition, info)
              _http_connect(...., broker._db_file) # db_file forms remote path
              broker.get_sync()
              _in_sync
                broker.merge_syncs()
              _rsync_db
                mtime = os.path.getmtime(broker._db_file)
                with broker.lock():
                  _rsync_file(broker._db_file, remote_file, False)
              _usync_db
                sync_table = broker.get_syncs()
                objects = broker.get_items_since(point, self.per_diff)
                broker.merge_syncs([....],....)
    class ReplicatorRpc():
      __init__(self,root,datadir,broker_class): self.broker_class=broker_class
      dispatch()
        db_file = os.path.join(self.root, drive,
                               storage_directory(self.datadir, partition, hsh),
                               hsh + '.db')
        return getattr(self, op)(self.broker_class(db_file), args)
      sync()
        info = broker.get_replication_info()
        quarantine_db(broker._db_file, broker.db_type)
        broker.update_metadata(simplejson.loads(metadata))
        broker.merge_timestamps(.... args of sync())
        info['point'] = broker.get_sync(id)
        broker.merge_syncs([{'remote_id': id, 'sync_point': remote_sync}])
      merge_syncs(self, broker, args):
        broker.merge_syncs(args[0])
      merge_items()
        broker.merge_items(args[0], args[1])
      complete_rsync(self, drive, db_file, args):
        broker = self.broker_class(old_filename)
        broker.newid(args[0])
      rsync_then_merge(self, drive, db_file, args):
        new_broker = self.broker_class(old_filename) # actually makes sense
        existing_broker = self.broker_class(db_file)
        objects = existing_broker.get_items_since(point, 1000)
        new_broker.merge_items(objects)
        new_broker.newid(args[0])

  test/unit/common/test_db.py:
    class TestAccountBroker(unittest.TestCase):
        broker = AccountBroker(':memory:', account='a')
        broker.get()
        broker.put_container('o', normalize_timestamp(time()), 0, 0, 0)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        broker.delete_db(normalize_timestamp(time()))
        info = broker.get_info()
        listing = broker.list_containers_iter(10, '3-0045', None, '3-', '-')

  swift/account/utils.py:
    account_listing_response
      if broker is None: broker = FakeAccountBroker()
      using get_info, metadata, list_containers_iter

  test/unit/common/test_db_replicator.py:
    creates a fake broker, tests db_replicator.Replicator, implements
    get_info, get_items_since, get_replication_info, get_sync, get_syncs,
    merge_items, merge_syncs, @contextmanager lock, reclaim

swift/common/db_replicator.py:
  class Replicator(Daemon):
    run_once/run_forver overloaded by AccountReplicator, ContainerReplicator
bin/swift-account-replicator:
  run_daemon(AccountReplicator, ....) # in swift/account/replicator.py
bin/swift-container-replicator:
  run_daemon(ContainerReplicator, ....) # in swift/container/replicator.py

swift/common/utils.py:
  audit_location_generator - os.listdir, os.listdir, os.listdir

-``

External Users in Code
----------------------

GlusterFS
  https://github.com/gluster/gluster-swift

API definition and practices
----------------------------

The ``class AccountBroker`` and ``class ContainerBroker`` are very similar
and inherit from the same class in the baseline implementation
(swift.common.db.DatabaseBroker). So, we consider them together,
calling them "Brocker".

* DatabaseBroker.__init__:

  |  def __init__(self, db_file, timeout=BROKER_TIMEOUT, logger=None,
  |               account=None, container=None, pending_timeout=10,
  |               stale_reads_ok=False):

  Instance variables that do not appear referred across API:

  |  self.conn = None
  |  self.pending_file = db_file + '.pending'
  |  self.db_dir = os.path.dirname(db_file)
  |  self.timeout = timeout
  |  self.logger = logger or logging.getLogger()
  |  self.account = account
  |  self.container = container
  |  self._db_version = -1

  Instance variables that are referred across API:

  | broker._db_file
  | broker.pending_timeout
  | broker.stale_reads_ok

  The broker._db_file has a patch, see below. Other two are work items.

  The broker.conn is not accessed directly, however refer to broker.get().

  Class variables:

  |  db_type = 'container'
  |  db_contains_type = 'object'

* __str__:
  
  |  def __str__(self):

  Added by c/28009 when hiding broker.db_file.
 
* _commit_puts(self, item_list=None):

* _delete_db(self, conn, timestamp):

* _initialize:

  Internal detail; overridden by ContainerBroker and AccountBroker as a way
  to share most of broker.initialize() code.

  In AccountBroker, calls broker.create_container_table() and
  broker.create_account_stat_table().

* _newid(self, conn):

  Same as newid() but assumes just-rsynched database.

  Implemented by ContainerBroker only, but DatabaseBroker provides a stub.
 
* _preallocate(self):

* _reclaim(self, conn, timestamp):

  Base

* can_delete_db(self, cutoff):

  Present in AccountBroker only.

* create_object_table(self, conn):

* create_container_stat_table(self, conn, put_timestamp=None):

* delete_db():

  Baseline has ``with self.get() as conn:`` here, an internal use of .get().

* delete_object(self, name, timestamp):

  Present in ContainerBroker only.

* empty()

  TBD: only applicatble to AccountBroker?

  Should be called is_empty(). Flushes pending updates, raising if not
  broker.stale_reads_ok (TBD: set when and where?). Selects container count.

* get_db_version(self, conn):

  Implemented by both broker types. Baseline code generates a version
  depending on other characteristics of the database: 0 or 1.

  Currenly only used internaly by the baseline implementation,
  considered not a part of API.

* get_info(self):

  Returns a dict.

  Returned keys for container:  account, container, created_at,
  put_timestamp, delete_timestamp, object_count, bytes_used,
  reported_put_timestamp, reported_delete_timestamp,
  reported_object_count, reported_bytes_used, hash, id,
  x_container_sync_point1, and x_container_sync_point2.

  Returned keys for account:  account, created_at, put_timestamp,
  delete_timestamp, container_count, object_count,
  bytes_used, hash, id.

  A side effect of get_info is quaranteening in case of problems.
  It is used by auditors.

* get_items_since(self, start, count):
 
* get_replication_info(self):

* get_sync(self, id, incoming=True):

* get_syncs(self, incoming=True):

* initialize:

  Creates a database in the baseline. Side effect: saves an open connection
  to database. GlusterFS works around lack of is_good() by leaving
  broker.initialize() empty.

  This can takes a special path ':memory:', is this used outside of tests?

  This can raise DatabaseAlreadyExists.

* is_deleted(self, timestamp=None):

  Only ContainerBroker implements the timestamp argument.
  TBD: how is the timestamp used? Race avoidance?

* is_good:

  Added by c/28009 when hiding broker.db_file. Basiline code is
  os.path.exists(self.db_file).

* is_status_deleted(self):

  Present in AccountBroker only.

* @contextmanager get(self):

  Returns a connection (yield conn). However, only used by tests.
  Ergo, implementations do not need to implement get() unless they
  aim to land in tree.

* @contextmanager lock(self):

* list_containers_iter(self, limit, marker, end_marker, prefix, delim):

  Present in AccountBroker only.

* list_objects_iter(self, limit, marker, end_marker, prefix, delim, path=None):

  Returns a list. TBD: could implementation return an interatable
  other than a list?

  Present in ContainerBroker only.

* merge_items(self, item_list, source=None):

* merge_timestamps(self, created_at, put_timestamp, delete_timestamp):
 
* merge_syncs(self, sync_points, incoming=True):

* @property metadata(self):

  metadata: A read/only property, can be emulated trivially in Python
  using a @property decorator. The baseline implementation does that
  and queries the database on every access. Thus, every access picks up
  the updates from other processes.

* newid(self, remote_id):

  Docstring: "Re-id the database.  This should be called after an rsync."

* possibly_quarantine:

  Examine and re-raise an exception. In the baseline, quarantine the DB
  if OSError.

* put_container():

  | def put_container(self, name, put_timestamp, delete_timestamp,
  |                   object_count, bytes_used)

  Present in AccountBroker only.

* put_object():

  | put_object(self, name, timestamp, size, content_type, etag, deleted=0):

* reclaim(self, object_timestamp, sync_timestamp):

  Actual brokers implement 2 timestamps.

* reclaim(self, timestamp):

  This is the base version with one timestamp only, seems a historic accident.
  See "Eliminate DatabaseBroker.reclaim":
  https://review.openstack.org/36176

* reported():

  | def reported(self, put_timestamp, delete_timestamp, object_count,
  |              bytes_used):

  Updates "reported stats". The baseline updates container_stat table with
  reported_bytes_used, reported_put_timestamp, etc.

  Present in ContainerBroker only.

* set_x_container_sync_points(self, sync_point1, sync_point2):

* _set_x_container_sync_points(self, conn, sync_point1, sync_point2):

* update_metadata(self, metadata_updates):

* update_put_timestamp(self, timestamp):


Class ``DiskFile`` provides an API to object server.

* DiskFile.__init__:

  variables XXX


====================
LFS: Planned Changes
====================

* Remove db_file from the API. Note that it is used for diagnostics a lot.
  There was some work done around the ".is_good() patch", but David Hadas
  put a -1 on it, holding hostage for some unrelated thing. See:
    https://review.openstack.org/28009
    https://review.openstack.org/26646

* The put_container is difficult to implement without a real database,
  because it has atomic lookup and update semantics. An implementation
  hast to find a record of specific container, subtract its stats from
  the account stats, then add new stats. All that is resistant to crashes
  and hangs, using database transactions.

* Remove or hide pending_timeout as implementation detail. TBD: How?

* Remove stale_reads_ok or define it strongly (unambiguously and future-proof).

* Change tests or else rename get() to _get(), since it's an internal API.

* Rearrange Swift tree so use of .initialize is logical (may require
  changing GlusterFS, TBD)
  * actually they already use .initialize now (7/7)

* Rename "delete_db" into "delete" and generally rename things to make
  it look less like they mandate or assume a database.

* Modify AccountController and ContainerController to load a configured
  plug-in directly, so inheriting them and overloading
  _get_account_broker and _get_container_broker is not longer needed.

  In DiskFile, Peter uses a settable class method currently, e.g.:

  | class ObjectController(object):
  |   def __init__(self, conf, disk_file_klass=None):
  |     if not disk_file_klass:
  |       disk_file_klass = DiskFile
  |     self.disk_file_klass = disk_file_klass
  |   def POST(self, request):
  |     disk_file = self.disk_file_klass(device, partition,
  |                                      account, container, obj,
  |                                      verify_existence=True)

TBD:

* API for replicator (swift/common/db_replicator.py) - outside of API? how?
* Container sync - is relevant or not? How to support?
* is_status_deleted() vs is_deleted() vs exists() or is_good(): doc, clarify
* Anything else that gropes through the DBs besides audit_location_generator
  and db_replicator.Replicator.run_once, walk_datadir, dispatch()?
* What metods other than get_info trigger quarantine, and is it used anywhere?
