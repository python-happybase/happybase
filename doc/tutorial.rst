********
Tutorial
********

.. py:currentmodule:: happybase

This tutorial explores the HappyBase API and should provide you with enough
information to get you started. Note that this tutorial is intended as an
introduction to HappyBase, not to HBase in general. Readers should already have
a basic understanding of HBase and its data model.

While the tutorial does cover most features, it is not a complete reference
guide. More information about the HappyBase API is available from the :doc:`API
documentation <api>`.

.. contents:: On this page
   :local:


Opening a :py:class:`Connection`
================================

We'll get started by connecting to HBase::

   import happybase

   connection = happybase.Connection('somehost')

When a :py:class:`Connection` instance is created, it automatically opens a
socket connection to the HBase Thrift server. This behaviour can be disabled by
setting the `autoconnect` argument to `False`, and opening the connection
manually using :py:meth:`Connection.open`::

   connection = happybase.Connection('somehost', autoconnect=False)

   # before first use:
   connection.open()

The :py:class:`Connection` class provides various methods to interact with the
HBase instance. For instance, we can ask ask for the names of the available
tables using the :py:meth:`Connection.tables` method::

   print connection.tables()

If a single HBase instance is used by multiple applications, table name
collisions may occur because applications use the same table names. A solution
is to add a ‘namespace’ prefix to the names of all tables ‘owned’ by a specific
application. Instead of adding this application-specific prefix each time a
table name is passed to HappyBase, the `table_prefix` parameter can be used.
HappyBase will prepend that prefix (and an underscore) to each table name
handled by the :py:class:`Connection` instance. So, for a project ``myproject``
that should have table names that look like ``myproject_XYZ``, use this::

   connection = happybase.Connection('somehost', table_prefix='myproject')

:py:meth:`Connection.tables` no longer includes tables in other ‘namespaces’;
it will only returns tables with a ``myproject_`` prefix in HBase, and also
strips of the prefix::

   print connection.tables()  # Table "myproject_XYZ" in HBase will be
                              # returned as simply "XYZ"

The :py:class:`Connection` class offers various other methods to interact with
HBase, mostly to perform table management tasks like enabling and disabling
tables. This tutorial does not cover those; the :doc:`API documentation <api>`
for the :py:class:`Connection` class contains more information.


Obtaining a :py:class:`Table` instance
======================================

The :py:class:`Table` class provides the main API to retrieve and manipulate
data in HBase. In the example above, we already asked for the available tables
using the :py:meth:`Connection.tables` method, so the next step is to obtain a
:py:class:`.Table` instance. This is done by calling
:py:meth:`Connection.table` with the name of the table::

   table = connection.table('mytable')

Obtaining a :py:class:`Table` instance does *not* result in a round-trip to the
Thrift server, which means application code may ask the :py:class:`Connection`
instance for a new :py:class:`Table` whenever it needs one, without negative
performance consequences. A side effect is that no check is done to ensure that
the table exists, since that would involve a round-trip, so expect errors if
you try to interact with non-existing tables later in your code. For this
tutorial, we assume the table exists.

.. note::

   The ‘heavy’ `HTable` HBase class from the Java HBase API, which does the
   real communication with the region servers, is at the other side of the
   Thrift connection. There is no direct mapping between :py:class:`Table`
   instances on the Python side and `HTable` instances on the server side.


Retrieving data
===============

The HBase data model is a multidimensional sparse map. A table in HBase
contains column families with column qualifiers containing a value and a
timestamp. In most of the HappyBase API, column family and qualifier names are
specified as a single string, e.g. ``cf1:col1``, and not as two separate
arguments. While column families and qualifiers are different concepts in the
HBase data model, they are almost always used together when interacting with
data, so treating them as a single string makes the API a lot simpler.

Retrieving rows
---------------

The :py:class:`Table` class offers various methods to retrieve data from a
table in HBase. The most basic one is :py:meth:`Table.row`, which retrieves a
single row from the table, and returns it as a dictionary mapping columns to
values::

   row = table.row('row-key')
   print row['cf1:col1']   # prints the value of cf1:col1

The :py:meth:`Table.rows` method works just like :py:meth:`Table.row`, but
takes multiple row keys and returns those as `(key, data)` tuples::

   rows = table.rows(['row-key-1', 'row-key-2'])
   for key, data in rows:
       print key, data

If you want the results that :py:meth:`Table.rows` returns as a dictionary or
ordered dictionary, you will have to do this yourself. This is really easy
though, since the return value can be passed directly to the dictionary
constructor. For a normal dictionary, order is lost::

   rows_as_dict = dict(table.rows(['row-key-1', 'row-key-2']))

…whereas for a :py:class:`OrderedDict`, order is preserved::

   from collections import OrderedDict
   rows_as_ordered_dict = OrderedDict(table.rows(['row-key-1', 'row-key-2']))


Making more fine-grained selections
-----------------------------------

HBase's data model allows for more fine-grained selections of the data to
retrieve. If you know beforehand which columns are needed, performance can be
improved by specifying those columns explicitly to :py:meth:`Table.row` and
:py:meth:`Table.rows`. The `columns` argument takes a list (or tuple) of column
names::

   row = table.row('row-key', columns=['cf1:col1', 'cf1:col2'])
   print row['cf1:col1']
   print row['cf1:col2']

Instead of providing both a column family and a column qualifier, items in the
`columns` argument may also be just a column family, which means that all
columns from that column family will be retrieved. For example, to get all
columns and values in the column family `cf1`, use this::

   row = table.row('row-key', columns=['cf1'])

In HBase, each cell has a timestamp attached to it. In case you don't want to
work with the latest version of data stored in HBase, the methods that retrieve
data from the database, e.g. :py:meth:`Table.row`, all accept a `timestamp`
argument that specifies that the results should be restricted to values with a
timestamp up to the specified timestamp::

   row = table.row('row-key', timestamp=123456789)

By default, HappyBase does not include timestamps in the results it returns. In
your application needs access to the timestamps, simply set the
`include_timestamp` parameter to ``True``. Now, each cell in the result will be
returned as a `(value, timestamp)` tuple instead of just a value::

   row = table.row('row-key', columns=['cf1:col1'], include_timestamp=True)
   value, timestamp = row['cf1:col1']

HBase supports storing multiple versions of the same cell. This can be
configured for each column family. To retrieve all versions of a column for a
given row, :py:meth:`Table.cells` can be used. This method returns an ordered
list of cells, with the most recent version coming first. The `versions`
argument specifies the maximum number of versions to return. Just like the
methods that retrieve rows, the `include_timestamp` argument determines whether
timestamps are included in the result. Example::

   values = table.cells('row-key', 'cf1:col1', versions=2)
   for value in values:
       print "Cell data: %s" % value

   cells = table.cells('row-key', 'cf1:col1', versions=3, include_timestamp=True)
   for value, timestamp in cells:
       print "Cell data at %d: %s" % (timestamp, value)

Note that the result may contain fewer cells than requested. The cell may just
have fewer versions, or you may have requested more versions than HBase keeps
for the column family.

Scanning over rows in a table
-----------------------------

In addition to retrieving data for known row keys, rows in HBase can be
efficiently iterated over using a table scanner, created using
:py:meth:`Table.scan`. A basic scanner that iterates over all rows in the table
looks like this::

   for key, data in table.scan():
       print key, data

Doing full table scans like in the example above is prohibitively expensive in
practice. Scans can be restricted in several ways to make more selective range
queries. One way is to specify start or stop keys, or both. To iterate over all
rows from row `aaa` to the end of the table::

   for key, data in table.scan(row_start='aaa'):
       print key, data

To iterate over all rows from the start of the table up to row `xyz`, use this::

   for key, data in table.scan(row_stop='xyz'):
       print key, data

To iterate over all rows between row `aaa` (included) and `xyz` (not included),
supply both::

   for key, data in table.scan(row_start='aaa', row_stop='xyz'):
       print key, data

An alternative is to use a key prefix. For example, to iterate over all rows
starting with `abc`::

   for key, data in table.scan(row_prefix='abc'):
       print key, data

The scanner examples above only limit the results by row key using the
`row_start`, `row_stop`, and `row_prefix` arguments, but scanners can also
limit results to certain columns, column families, and timestamps, just like
:py:meth:`Table.row` and :py:meth:`Table.rows`. For advanced users, a filter
string can be passed as the `filter` argument. Additionally, the optional
`limit` argument defines how much data is at most retrieved, and the
`batch_size` argument specifies how big the transferred chunks should be. The
:py:meth:`Table.scan` API documentation provides more information on the
supported scanner options.


Manipulating data
=================

In HBase, all mutations either store data or mark data for deletion; there is
no such thing as an `update`. HappyBase provides methods to do single inserts
or deletes, and also a batch API for bulk mutations.

Storing data
------------

To store a single cell of data in our table, we can use :py:meth:`Table.put`,
which takes the row key, and the data to store. The data should be a dictionary
mapping the column name to a value::

   table.put('row-key', {'cf:col1': 'value1',
                         'cf:col2': 'value2'})

Use the `timestamp` argument if you want to provide timestamps explicitly::

   table.put('row-key', {'cf:col1': 'value1'}, timestamp=123456789)

If omitted, HBase defaults to the current system time.

Deleting data
-------------

The :py:meth:`Table.delete` method deletes data from a table. To delete a
complete row, just specify the row key::

   table.delete('row-key')

To delete one or more columns instead of a complete row, also specify the
`columns` argument::

   table.delete('row-key', columns=['cf1:col1', 'cf1:col2'])

The optional `timestamp` argument restricts the delete operation to data up to
the specified timestamp.

Performing batch mutations
--------------------------

The :py:meth:`Table.put` and :py:meth:`Table.delete` methods both issue a
command to the HBase Thrift server immediately. This means that using these
methods is not very efficient when storing or deleting multiple values. It is
much more efficient to aggregate a bunch of commands and send them to the
server in one go. This is exactly what the :py:class:`Batch` class, created
using :py:meth:`Table.batch`, does. A :py:class:`Batch` instance has put and
delete methods, just like the :py:class:`Table` class, but the changes are sent
to the server in a single round-trip using :py:meth:`Batch.send`::

   b = table.batch()
   b.put('row-key-1', {'cf:col1': 'value1', 'cf:col2': 'value2'})
   b.put('row-key-2', {'cf:col2': 'value2', 'cf:col3': 'value3'})
   b.put('row-key-3', {'cf:col3': 'value3', 'cf:col4': 'value4'})
   b.delete('row-key-4')
   b.send()

.. note::

   Storing and deleting data for the same row key in a single batch leads to
   unpredictable results, so don't do that.

While the methods on the :py:class:`Batch` instance resemble the
:py:meth:`~Table.put` and :py:meth:`~Table.delete` methods, they do not take a
`timestamp` argument for each mutation. Instead, you can specify a single
`timestamp` argument for the complete batch::

   b = table.batch(timestamp=123456789)
   b.put(...)
   b.delete(...)
   b.send()

:py:class:`Batch` instances can be used as *context managers*, which are most
useful in combination with Python's ``with`` construct. The example above can
be simplified to read::

   with table.batch() as b:
       b.put('row-key-1', {'cf:col1': 'value1', 'cf:col2': 'value2'})
       b.put('row-key-2', {'cf:col2': 'value2', 'cf:col3': 'value3'})
       b.put('row-key-3', {'cf:col3': 'value3', 'cf:col4': 'value4'})
       b.delete('row-key-4')

As you can see, there is no call to :py:meth:`Batch.send` anymore. The batch is
automatically applied when the ``with`` code block terminates, even in case of
errors somewhere in the ``with`` block, so it behaves basically the same as a
``try/finally`` clause. However, some applications require transactional
behaviour, sending the batch only if no exception occurred. Without a context
manager this would look something like this::

   b = table.batch()
   try:
       b.put('row-key-1', {'cf:col1': 'value1', 'cf:col2': 'value2'})
       b.put('row-key-2', {'cf:col2': 'value2', 'cf:col3': 'value3'})
       b.put('row-key-3', {'cf:col3': 'value3', 'cf:col4': 'value4'})
       b.delete('row-key-4')
       raise ValueError("Something went wrong!")
   except ValueError as e:
       # error handling goes here; nothing is sent to HBase
       pass
   else:
       # no exceptions; send data
       b.send()

Obtaining the same behaviour is easier using a ``with`` block. The
`transaction` argument to :py:meth:`Table.batch` is all you need::

   try:
       with table.batch(transaction=True) as b:
           b.put('row-key-1', {'cf:col1': 'value1', 'cf:col2': 'value2'})
           b.put('row-key-2', {'cf:col2': 'value2', 'cf:col3': 'value3'})
           b.put('row-key-3', {'cf:col3': 'value3', 'cf:col4': 'value4'})
           b.delete('row-key-4')
           raise ValueError("Something went wrong!")
   except ValueError:
       # error handling goes here; nothing is sent to HBase
       pass

   # when no error occurred, the transaction succeeded

As you may have imagined already, a :py:class:`Batch` keeps all mutations in
memory until the batch is sent, either by calling :py:meth:`Batch.send()`
explicitly, or when the ``with`` block ends. This doesn't work for applications
that need to store huge amounts of data, since it may result in batches that
are too big to send in one round-trip, or in batches that use too much memory.
For these cases, the `batch_size` argument can be specified. The `batch_size`
acts as a threshold: a :py:class:`Batch` instance automatically sends all
pending mutations when there are more than `batch_size` pending operations. For
example, this will result in three round-trips to the server (two batches with
1000 cells, and one with the remaining 400)::

   with table.batch(batch_size=1000) as b:
       for i in range(1200):
           # this put() will result in two mutations (two cells)
           b.put('row-%04d' % i, {'cf1:col1': 'v1',
                                  'cf1:col2': 'v2',})

The appropriate `batch_size` is very application-specific since it depends on
the data size, so just experiment to see how different sizes work for your
specific use case.

Using atomic counters
---------------------

The :py:meth:`Table.counter_inc` and :py:meth:`Table.counter_dec` methods allow
for atomic incrementing and decrementing of 8 byte wide values, which are
interpreted as big-endian 64-bit signed integers by HBase. Counters are
automatically initialised to 0 upon first use. When incrementing or
decrementing a counter, the value after modification is returned. Example::

   print table.counter_inc('row-key', 'cf1:counter')  # prints 1
   print table.counter_inc('row-key', 'cf1:counter')  # prints 2
   print table.counter_inc('row-key', 'cf1:counter')  # prints 3

   print table.counter_dec('row-key', 'cf1:counter')  # prints 2

The optional `value` argument specifies how much to increment or decrement by::

   print table.counter_inc('row-key', 'cf1:counter', value=3)  # prints 5

While counters are typically used with the increment and decrement functions
shown above, the :py:meth:`Table.counter_get` and :py:meth:`Table.counter_set`
methods can be used to retrieve or set a counter value directly::

   print table.counter_get('row-key', 'cf1:counter')  # prints 5

   table.counter_set('row-key', 'cf1:counter', 12)

Note that an application should *never* :py:meth:`~Table.counter_get` the
current value, modify it in code and then :py:meth:`~Table.counter_set` the
modified value; use the atomic :py:meth:`~Table.counter_inc` and
:py:meth:`~Table.counter_dec` instead!

.. vim: set spell spelllang=en:
