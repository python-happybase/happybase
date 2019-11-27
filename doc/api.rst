=============
API reference
=============

.. py:currentmodule:: aiohappybase

This chapter contains detailed API documentation for AIOHappyBase. It is suggested to read the :doc:`user guide <user>` first to get a general idea about how AIOHappyBase works.

The AIOHappyBase API is organised as follows:

:py:class:`~aiohappybase.Connection`:
   The :py:class:`~aiohappybase.Connection` class is the main entry point for
   application developers. It connects to the HBase Thrift server and provides
   methods for table management.

:py:class:`~aiohappybase.Table`:
   The :py:class:`Table` class is the main class for interacting with data in
   tables. This class offers methods for data retrieval and data manipulation.
   Instances of this class can be obtained using the
   :py:meth:`Connection.table()` method.

:py:class:`~aiohappybase.Batch`:
   The :py:class:`Batch` class implements the batch API for data manipulation,
   and is available through the :py:meth:`Table.batch()` method.

:py:class:`~aiohappybase.ConnectionPool`:
   The :py:class:`ConnectionPool` class implements a thread-safe connection
   pool that allows an application to (re)use multiple connections.


Connection
==========

.. autoclass:: aiohappybase.Connection


Table
=====

.. autoclass:: aiohappybase.Table


Batch
=====

.. autoclass:: aiohappybase.Batch


Connection pool
===============

.. autoclass:: aiohappybase.ConnectionPool

.. autoclass:: aiohappybase.NoConnectionsAvailable


.. vim: set spell spelllang=en:
