Version history
===============

.. py:currentmodule:: happybase


HappyBase 1.2.0
---------------

Release date: 2019-05-14

* Switch from ``thriftpy`` to its successor ``thriftpy2``,
  which supports Python 3.7.
  (`issue #221 <https://github.com/wbolster/happybase/issues/221>`_,
  `pr 222 <https://github.com/wbolster/happybase/pull/222>`_,


HappyBase 1.1.0
---------------

Release date: 2017-04-03

* Set socket timeout unconditionally on ``TSocket``
  (`#146 <https://github.com/wbolster/happybase/issues/146>`_)

* Add new ‘0.98’ compatibility mode
  (`#155 <https://github.com/wbolster/happybase/issues/155>`_)

* Add support for reversed scanners
  (`#67 <https://github.com/wbolster/happybase/issues/67>`_,
  `#155 <https://github.com/wbolster/happybase/issues/155>`_)


HappyBase 1.0.0
---------------

Release date: 2016-08-13

* First 1.x.y release!

  From now on this library uses a semantic versioning scheme.
  HappyBase is a mature library, but always used 0.x version numbers
  for no good reason. This has now changed.

* Finally, Python 3 support. Thanks to all the people who contributed!
  (`issue #40 <https://github.com/wbolster/happybase/issues/40>`_,
  `pr 116 <https://github.com/wbolster/happybase/pull/116>`_,
  `pr 108 <https://github.com/wbolster/happybase/pull/108>`_,
  `pr 111 <https://github.com/wbolster/happybase/pull/111>`_)

* Switch to thriftpy as the underlying Thrift library, which is a much
  nicer and better maintained library.

* Enable building universal wheels
  (`issue 78 <https://github.com/wbolster/happybase/pull/78>`_)


HappyBase 0.9
-------------

Release date: 2014-11-24

* Fix an issue where scanners would return fewer results than expected due to
  HBase not always behaving as its documentation suggests (`issue #72
  <https://github.com/wbolster/happybase/issues/72>`_).

* Add support for the Thrift compact protocol (``TCompactProtocol``) in
  :py:class:`Connection` (`issue #70
  <https://github.com/wbolster/happybase/issues/70>`_).


HappyBase 0.8
-------------

Release date: 2014-02-25

* Add (and default to) '0.96' compatibility mode in :py:class:`Connection`.

* Add support for retrieving sorted columns, which is possible with the HBase
  0.96 Thrift API. This feature uses a new `sorted_columns` argument to
  :py:meth:`Table.scan`. An ``OrderedDict`` implementation is required for this
  feature; with Python 2.7 this is available from the standard library, but for
  Python 2.6 a separate ``ordereddict`` package has to be installed from PyPI.
  (`issue #39 <https://github.com/wbolster/happybase/issues/39>`_)

* The `batch_size` argument to :py:meth:`Table.scan` is no longer propagated to
  `Scan.setBatching()` at the Java side (inside the Thrift server). To influence
  the `Scan.setBatching()` (which may split rows into partial rows) a new
  `scan_batching` argument to :py:meth:`Table.scan` has been added. See `issue
  #54 <https://github.com/wbolster/happybase/issues/54>`_, `issue #56
  <https://github.com/wbolster/happybase/issues/56>`_, and the HBase docs for
  `Scan.setBatching()` for more details.


HappyBase 0.7
-------------

Release date: 2013-11-06

* Added a `wal` argument to various data manipulation methods on the
  :py:class:`Table` and :py:class:`Batch` classes to determine whether to write
  the mutation to the Write-Ahead Log (WAL). (`issue #36
  <https://github.com/wbolster/happybase/issues/36>`_)

* Pass batch_size to underlying Thrift Scan instance (`issue #38
  <https://github.com/wbolster/happybase/issues/38>`_).

* Expose server name and port in :py:meth:`Table.regions` (recent HBase versions
  only) (`issue #37 <https://github.com/wbolster/happybase/issues/37>`_).

* Regenerated bundled Thrift API modules using a recent upstream Thrift API
  definition. This is required to expose newly added API.


HappyBase 0.6
-------------

Release date: 2013-06-12

* Rewrote exception handling in connection pool. Exception handling is now a lot
  cleaner and does not introduce cyclic references anymore. (`issue #25
  <https://github.com/wbolster/happybase/issues/25>`_).

* Regenerated bundled Thrift code using Thrift 0.9.0 with the new-style classes
  flag (`issue #27 <https://github.com/wbolster/happybase/issues/27>`_).


HappyBase 0.5
-------------

Release date: 2013-05-24

* Added a thread-safe connection pool (:py:class:`ConnectionPool`) to keep
  connections open and share them between threads (`issue #21
  <https://github.com/wbolster/happybase/issues/21>`_).

* The :py:meth:`Connection.delete_table` method now features an optional
  `disable` parameter to make deleting enabled tables easier.

* The debug log message emitted by :py:meth:`Table.scan` when closing a scanner
  now includes both the number of rows returned to the calling code, and also
  the number of rows actually fetched from the server. If scanners are not
  completely iterated over (e.g. because of a 'break' statement in the for loop
  for the scanner), these numbers may differ. If this happens often, and the
  differences are big, this may be a hint that the `batch_size` parameter to
  :py:meth:`Table.scan()` is not optimal for your application.

* Increased Thrift dependency to at least 0.8. Older versions are no longer
  available from PyPI. HappyBase should not be used with obsoleted Thrift
  versions.

* The :py:class:`Connection` constructor now features an optional `timeout`
  parameter to to specify the timeout to use for the Thrift socket (`issue #15
  <https://github.com/wbolster/happybase/issues/15>`_)

* The `timestamp` argument to various methods now also accepts `long` values in
  addition to `int` values. This fixes problems with large timestamp values on
  32-bit systems. (`issue #23
  <https://github.com/wbolster/happybase/issues/23>`_).

* In some corner cases exceptions were raised during interpreter shutdown while
  closing any remaining open connections. (`issue #18
  <https://github.com/wbolster/happybase/issues/18>`_)


HappyBase 0.4
-------------

Release date: 2012-07-11

* Add an optional `table_prefix_separator` argument to the
  :py:class:`Connection` constructor, to specify the prefix used for the
  `table_prefix` argument (`issue #3
  <https://github.com/wbolster/happybase/issues/3>`_)
* Add support for framed Thrift transports using a new optional `transport`
  argument to :py:class:`Connection` (`issue #6
  <https://github.com/wbolster/happybase/issues/6>`_)
* Add the Apache license conditions in the :doc:`license statement <license>`
  (for the included HBase parts)
* Documentation improvements


HappyBase 0.3
-------------

Release date: 2012-05-25

New features:

* Improved compatibility with HBase 0.90.x

  * In earlier versions, using :py:meth:`Table.scan` in combination with HBase
    0.90.x often resulted in crashes, caused by incompatibilities in the
    underlying Thrift protocol.
  * A new `compat` flag to the :py:class:`Connection` constructor has been
    added to enable compatibility with HBase 0.90.x.
  * Note that the :py:meth:`Table.scan` API has a few limitations when used
    with HBase 0.90.x.

* The `row_prefix` argument to :py:meth:`Table.scan` can now be used together
  with `filter` and `timestamp` arguments.

Other changes:

* Lower Thrift dependency to 0.6
* The `setup.py` script no longer installs the tests
* Documentation improvements


HappyBase 0.2
-------------

Release date: 2012-05-22

* Fix package installation, so that ``pip install happybase`` works as expected
  (`issue #1 <https://github.com/wbolster/happybase/issues/1>`_)
* Various small documentation improvements


HappyBase 0.1
-------------

Release date: 2012-05-20

* Initial release
