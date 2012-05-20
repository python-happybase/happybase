************
Introduction
************

.. py:currentmodule:: happybase

.. contents:: On this page
   :local:


What is HappyBase?
==================

.. include:: ../README.rst

HappyBase is designed for for use in standard HBase setups, and offers
application developers a Pythonic API to interact with HBase.

Below the surface, HappyBase uses the `Python Thrift library
<http://pypi.python.org/pypi/thrift>`_ to connect to HBase's `Thrift
<http://thrift.apache.org/>`_ gateway, which is included in the standard HBase
0.9x releases. HappyBase hides most of the details of the underlying RPC
mechanisms, resulting in application code that is cleaner, more productive to
write, and more maintainable.


What does code using HappyBase look like?
=========================================

The example below illustrates basic usage of the library::

   import happybase

   connection = happybase.Connection('hostname')
   table = connection.table('table-name')

   table.put('row-key', {'family:qual1': 'value1',
                         'family:qual2': 'value2'})

   row = table.row('row-key')
   print row['family:qual1']  # prints 'value1'

   for key, data in table.rows(['row-key-1', 'row-key-2']):
       print key, data  # prints row key and data for each row

   for key, data in table.scan(row_prefix='row'):
       print key, data  # prints 'value1' and 'value2'

   row = table.delete('row-key')

Note that the :doc:`tutorial <tutorial>` contains many more examples.


Why not use the HBase Thrift API directly?
==========================================

You may consider using the HBase Thrift API directly instead of adding yet
another library to your project. After all, :pep:`20` taught us that simple is
better than complex, and there should be one, and preferably one way to do it,
right? Well, we agree.

While the HBase Thrift API can be used directly from Python using the
(automatically generated) HBase Thrift service classes, application code using
this API is verbose, cumbersome, and hence error-prone. The reason for this is
that the HBase Thrift API is a flat, language-agnostic interface API closely
tied to the RPC going over the wire-level protocol. This means that
applications need to deal with many imports, sockets, transports, protocols,
clients, Thrift types and mutation objects. For instance, look at the code
required to connect to HBase and store two values::

   from thrift import Thrift
   from thrift.transport import TSocket, TTransport
   from thrift.protocol import TBinaryProtocol

   from hbase import ttypes
   from hbase.Hbase import Client, Mutation

   sock = TSocket.TSocket('hostname', 9090)
   transport = TTransport.TBufferedTransport(sock)
   protocol = TBinaryProtocol.TBinaryProtocol(transport)
   client = Client(protocol)
   transport.open()

   mutations = [Mutation(column='family:qual1', value='value1'),
                Mutation(column='family:qual2', value='value2')]
   client.mutateRow('table-name', 'row-key', mutations)

HappyBase hides all the Thrift cruft below a friendly API, and makes the task
in the example above look like this::

   import happybase
   connection = happybase.Connection('hostname')
   table = connection.table('table-name')
   table.put('row-key', {'family:qual1': 'value1',
                         'family:qual2': 'value2'})

Hopefully this example makes it clear that you will be a lot happier using
HappyBase than using the Thrift API directly. If you still have doubts about
this, try to accomplish some other common tasks, e.g. retrieving rows and
scanning over a part of a table, and compare that with the really-easy-to-use
HappyBase equivalents. If you're still not convinced by then, we're sorry to
inform you that HappyBase is not the project for you, and we wish you all of
luck maintaining your code ‒ or is it Thrift boilerplate? ‒ while your
application evolves.


How do I get started?
=====================

Follow the :doc:`installation guide <installation>` and read the :doc:`tutorial
<tutorial>`.


.. vim: set spell spelllang=en:
