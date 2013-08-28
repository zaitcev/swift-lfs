=================================
Pluggable Back-ends: Evolving API
=================================

API definition and practices
----------------------------

The Back-end API is based on pre-existing classes, used by Account, Container,
and Object servers: ``class AccountBroker``, ``class ContainerBroker``,
and ``class DiskFile``, respectively.

The broker classes are very similar and inherit from the same class in
the baseline implementation (:class:`swift.common.db.DatabaseBroker`).
So, we consider them together, calling them "Broker".
See :ref:`db` for the relevant automatically generated documentation.

See :ref:`disk_file` for the automatically generated documentation
on the ``class DiskFile``.

Servers load the desired back-end from a Python egg by calling
a utility function :func:`get_broker`. They are configured by setting
a desirable "group name" in `foo-server.conf`. For example:

  | [DEFAULT]
  | lfs_broker_group = megafs.swift.lfs_broker

If the group is not configured, the baseline back-end is loaded.
This way Swift may be upgraded and use existing configs safely.

An independent back-end typically includes the following in
its setup.cfg:

  | openstack.swift.lfs_broker =
  |     account = swift.common.db:AccountBroker
  |     container = swift.common.db:ContainerBroker
  |     object = swift.obj.diskfile:DiskFile


The group names are presumed unique among all installed eggs for
our purposes. The 'account', 'container', and 'object' selectors
are hard-coded in Swift servers.

DatabaseBroker
--------------

* DatabaseBroker.__init__:

  GlusterFS UFO always ignores stale_reads_ok argument to DiskDir.__init__()
  and sets the instance variable to False. This worked up to 1.9.1 because
  the controllers never passed stale_reads_ok argument, but set it with
  assignement only. This is not a safe assumption anymore.

* __str__:

  |  def __str__(self):

  Added by review 28009 when hiding broker.db_file.

* delete_db():

  Baseline has ``with self.get() as conn:`` here, an internal use of .get().

* empty()

  Should be called is_empty(). Flushes pending updates, raising if not
  broker.stale_reads_ok. Fetches container or object count out of the DB.

* initialize:

  Creates a database in the baseline. Side effect: saves an open connection
  to database. GlusterFS works around lack of is_good() by leaving
  broker.initialize() empty.

  This can takes a special path ':memory:', is this used outside of tests?

  This can raise DatabaseAlreadyExists.

* is_deleted(self, timestamp=None):

  Only ContainerBroker implements the timestamp argument.
  TBD: how is the timestamp used? Race avoidance?

  Flushes pending updates, raising LockTimeout if not broker.stale_reads_ok.

* is_good:

  Added by review 28009 when hiding broker.db_file. Baseline code is
  os.path.exists(self.db_file).

* is_status_deleted(self):

  Present in AccountBroker only.

* @contextmanager get(self):

  Returns a connection (``yield conn``). However, only used by tests.
  Ergo, implementations do not need to implement get() unless they
  aim to land in tree.

* @contextmanager lock(self):

* list_objects_iter(self, limit, marker, end_marker, prefix, delim, path=None):

  Returns a list. TBD: could implementation return an interatable
  other than a list?

  Present in ContainerBroker only.

  Flushes pending updates, raising LockTimeout if not broker.stale_reads_ok.

* reclaim(self, timestamp):

  This is the base version with one timestamp only, seems a historic accident.
  See "Eliminate DatabaseBroker.reclaim":
  https://review.openstack.org/36176

DiskFile
--------

* DiskFile.__init__:

  | def __init__(self, path, device, partition, account, container, obj,
  |              logger, keep_data_fp=False, disk_chunk_size=65536,
  |              bytes_per_sync=(512 * 1024 * 1024), iter_hook=None,
  |              threadpool=None):

  Instance variables that are referred across API:

  | self.quarantined_dir = None  -- used in auditor
  | self.data_file = None        -- used in auditor, change to exists()
  | self.keep_cache = False  -- used in server

  DiskFile may have the object pre-opened (possibly for no good reason
  in the baseline code), and has no __del__, so .close() should be called
  before disposing.

  The meta_file is going to be made a local variable during refactoring.

  The metadata is a read-only property, but it is a real property in
  ``DiskFile``, not overridden with @property decorator.

* get_data_file_size(self):

    This is to be dropped. See https://review.openstack.org/34811

* writer(self, size=None):

  Returns an instance of ``class DiskWriter``.

Class ``DiskWriter`` is returned by DiskFile.writer() and tracks
the state of an object being written, including things like total bytes
and the running MD5 sum.

A ``DiskFile`` and a ``DiskWriter`` of an implementation go together
and thus refer to each other's internals, such as ``DiskFile.name``,
not a part of API.

* DiskWriter.__init__:

  |  def __init__(self, disk_file, fd, tmppath, threadpool)

  |  self.disk_file = disk_file

  The parent DiskFile class.

  |  self.fd = fd
  |  self.tmppath = tmppath
  |  self.upload_size = 0
  |  self.last_sync = 0
  |  self.threadpool = threadpool

  Not invoked directly by Swift code, so not a part of Back-end API.

* write():

  |  def write(self, chunk)

* put():

  |  def put(self, metadata)

  This is API definition. The baseline implementation may have an extra
  argument, when write calls put(), but this is not part of its API.
  Implementors of back-ends only need to implement the definition above.

Class ``DiskReader`` is not present in Swift 1.9.0.
See review https://review.openstack.org/35381

External Users
--------------

GlusterFS
  https://github.com/gluster/gluster-swift

======================================
Pluggable Back-ends: API Documentation
======================================

.. automodule:: swift.common.backends
    :private-members:
    :members:
    :undoc-members:


====================================
Pluggable Back-ends: Planned Changes
====================================

* Remove db_file from the API. Note that it is used for diagnostics a lot.
  There was some work done around the ".is_good() patch", not complete yet
  due to a -1.

  See review https://review.openstack.org/28009
  and https://review.openstack.org/26646

* The put_container is difficult to implement without a real database,
  because it has atomic lookup and update semantics. An implementation
  hast to find a record of specific container, subtract its stats from
  the account stats, then add new stats. All that is resistant to crashes
  and hangs, using database transactions.

* Remove or hide pending_timeout as implementation detail.

  See review https://review.openstack.org/39588 (merged)

* Move stale_reads_ok to argument or inside of _get_account_broker.
 
  See review https://review.openstack.org/36919 (merged)

* Change tests or else rename get() to _get(), since it's an internal API.

* Rearrange Swift tree so use of .initialize is logical (may require
  changing GlusterFS, TBD)
  * actually they already use .initialize now (7/7)

* Rename "delete_db" into "delete" and generally rename things to make
  it look less like they mandate or assume a database.

* Modify AccountController and ContainerController to load a configured
  plug-in directly, so inheriting them and overloading
  _get_account_broker and _get_container_broker is not longer needed.

  See review https://review.openstack.org/40037

* Move mount checking into _diskfile()

  This relieves the implementation from working around the checking done
  by the core Swift.
  See review https://review.openstack.org/35505

* The mount check also exists in DB broker, needs hiding in the same way
  as Peter did for DiskFile.

* Eliminate can_delete_db.

  See review https://review.openstack.org/39193 (merged)

* Split backends.py away from db.py

TBD:

* API for replicator (swift/common/db_replicator.py) - outside of API? how?
* Container sync - is relevant or not? How to support?
* is_status_deleted() vs is_deleted() vs exists() or is_good(): doc, clarify
* Anything else that gropes through the DBs besides audit_location_generator
  and db_replicator.Replicator.run_once, walk_datadir, dispatch()?
* What metods other than get_info trigger quarantine, and is it used anywhere?
* DiskFile.suppress_file_closing is ugly, but is not an API problem.
  Kill it now, or ignore until better times? Is it linked to Peter's "wart"
  and open() outside of DiskReader?
* Peter's DiskFile.usage has _quarantined_dir with underscore, is this
  in already? What review number?
* DB_PREALLOCATION must be fixed up
  ``swift.common.db.DB_PREALLOCATION = ... conf.get('db_preallocation', 'f')``
