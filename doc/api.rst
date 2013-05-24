=============
API reference
=============

.. py:currentmodule:: happybase

This chapter contains detailed API documentation for HappyBase. It is suggested
to read the :doc:`user guide <user>` first to get a general idea about how
HappyBase works.

The HappyBase API is organised as follows:

:py:class:`~happybase.Connection`:
   The :py:class:`~happybase.Connection` class is the main entry point for
   application developers. It connects to the HBase Thrift server and provides
   methods for table management.

:py:class:`~happybase.Table`:
   The :py:class:`Table` class is the main class for interacting with data in
   tables. This class offers methods for data retrieval and data manipulation.
   Instances of this class can be obtained using the
   :py:meth:`Connection.table()` method.

:py:class:`~happybase.Batch`:
   The :py:class:`Batch` class implements the batch API for data manipulation,
   and is available through the :py:meth:`Table.batch()` method.

:py:class:`~happybase.ConnectionPool`:
   The :py:class:`ConnectionPool` class implements a thread-safe connection
   pool that allows an application to (re)use multiple connections.


Connection
==========

.. autoclass:: happybase.Connection


Table
=====

.. autoclass:: happybase.Table


Batch
=====

.. autoclass:: happybase.Batch


Connection pool
===============

.. autoclass:: happybase.ConnectionPool

.. autoclass:: happybase.NoConnectionsAvailable


.. vim: set spell spelllang=en:
