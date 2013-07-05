Existing API
============

* Internal Users in Code *

DatabaseBroker:
  test/unit/common/test_db.py
  # not used by anyone directly

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
    brokerclass = db.ContainerBroker # used later from bin/ somehow TBD
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
    db_replicator.Replicator.brokerclass
    # TBD study how the brokerclass indirection works; likely trivial

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
      brokerclass = db.AccountBroker
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
        info['point'] = broker.get_sync(id_)
        broker.merge_syncs([{'remote_id': id_, 'sync_point': remote_sync}])
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
        broker.get() # TBD: is get() used in real code at all?
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

* External Users in Code *

GlusterFS
  https://github.com/gluster/gluster-swift
  ??????

* API notes *

broker.get_info()
 - side effect is quaranteening in case of problems, used by auditor
broker.pending_timeout = 0.1
broker.stale_reads_ok = True
broker.metadata - read/only property, can be emulated trivially in Python

Planned Changes
===============

- db_file
   is_good - Hadas -1, holding hostage for some unrelated thing
     TBD review #s for both
- put_xxx ??
- pending_timeout = 0.1
- stale_reads_ok
- if broker.get is internal API, change tests or else rename _get()

TBD: API for replicator (swift/common/db_replicator.py) - outside of API? how?
TBD: Container sync - is relevant or not? How to support?
TBD: is_status_deleted() vs is_deleted() vs exists() or is_good(): doc, clarify
TBD: anything else that gropes through the DBs besides audit_location_generator
     and db_replicator.Replicator.run_once, walk_datadir, dispatch()?
