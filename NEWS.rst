.. Note: this list is automatically included in the documentation.

***************
Version history
***************

.. py:currentmodule:: happybase

HappyBase 0.3 (not yet released)
================================

Feature changes:

* Add compatibility for HBase 0.90, with (slightly) limited scanner functionality.
* The `row_prefix` argument to :py:meth:`Table.scan` can now be used together with `filter` and `timestamp` arguments.

Other changes:

* Lower Thrift dependency to 0.6
* The `setup.py` script no longer installs the tests
* Documentation improvements


HappyBase 0.2
=============

Release date: 2012-05-22

* Fix package installation, so that ``pip install happybase`` works as expected
* Various small documentation improvements


HappyBase 0.1
=============

Release date: 2012-05-20

* Initial release
