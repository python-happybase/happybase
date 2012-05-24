Version history
===============

.. py:currentmodule:: happybase

HappyBase 0.3 (not yet released)
--------------------------------

New features:

* Improved compatibility with HBase 0.90.x

  * In earlier versions, using :py:meth:`Table.scan` in combination with HBase
    0.90.x often resulted in crashes, caused by incompatibilities the
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
* Various small documentation improvements


HappyBase 0.1
-------------

Release date: 2012-05-20

* Initial release
