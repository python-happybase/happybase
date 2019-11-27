==========
User guide
==========

.. py:currentmodule:: aiohappybase

This user guide explores the AIOAIOHappyBase API and should provide you with 
enough information to get you started. Note that this user guide is intended as 
an introduction to AIOAIOHappyBase, not to HBase in general. Readers should 
already have a basic understanding of HBase and its data model.

While the user guide does cover most features, it is not a complete reference
guide. More information about the AIOAIOHappyBase API is available from the 
:doc:`API documentation <api>`.

.. contents:: On this page
   :local:


Establishing a connection
=========================

We'll get started by connecting to HBase. Just create a new
:py:class:`Connection` instance::

    from aiohappybase import Connection

    connection = Connection('somehost')

    # Or better, to ensure the connection is closed later

    async with Connection('somehost') as connection:
        # Do your thing

In some setups, the :py:class:`Connection` class needs some additional
information about the HBase version it will be connecting to, and which Thrift
transport to use. If you're still using HBase 0.90.x, you need to set the
`compat` argument to make sure AIOHappyBase speaks the correct wire protocol.
Additionally, if you're using HBase 0.94 with a non-standard Thrift transport
mode, make sure to supply the right `transport` argument. See the API
documentation for the :py:class:`Connection` class for more information about
these arguments and their supported values.

When a :py:class:`Connection` is created, it can automatically open a socket
connection to the HBase Thrift server if `autoconnect` is set to True
or the :py:class:`Connection` is created using a context.

The :py:class:`Connection` class provides the main entry point to interact with
HBase. For instance, to list the available tables, use
:py:meth:`Connection.tables`::

   print(await connection.tables())

Most other methods on the :py:class:`Connection` class are intended for system
management tasks like creating, dropping, enabling and disabling tables. See the
:doc:`API documentation <api>` for the :py:class:`Connection` class contains
more information. This user guide does not cover those since it's more likely
you are already using the HBase shell for these system management tasks.

.. note::

   AIOHappyBase also features a connection pool, which is covered later in this
   guide.


Working with tables
===================

The :py:class:`Table` class provides the main API to retrieve and manipulate
data in HBase. In the example above, we already asked for the available tables
using the :py:meth:`Connection.tables` method. If there weren't any tables yet,
you can create a new one using :py:meth:`Connection.create_table`::

    table = await connection.create_table('mytable', {
        'cf1': dict(max_versions=10),
        'cf2': dict(max_versions=1, block_cache_enabled=False),
        'cf3': dict(),  # use defaults
    })

.. note::

    The HBase shell is often a better alternative for many HBase administration
    tasks, since the shell is more powerful compared to the limited Thrift API
    that AIOHappyBase uses.

If the table already exists, you can get a :py:class:`.Table` instance to
work with by simply calling :py:meth:`Connection.table`, and passing it the
table name::

    table = connection.table('mytable')

Note that this method *is not async*. Obtaining a :py:class:`Table` instance
does *not* result in a round-trip to the Thrift server, which means application
code may ask the :py:class:`Connection` instance for a new :py:class:`Table`
whenever it needs one, without negative performance consequences. A side effect
is that no check is done to ensure that the table exists, since that would
involve a round-trip. Expect errors if you try to interact with non-existing
tables later in your code. For this guide, we assume the table exists.

.. note::

   The ‘heavy’ `HTable` HBase class from the Java HBase API, which performs the
   real communication with the region servers, is at the other side of the
   Thrift connection. There is no direct mapping between :py:class:`Table`
   instances on the Python side and `HTable` instances on the server side.

Using table ‘namespaces’
------------------------

If a single HBase instance is shared by multiple applications, table names used
by different applications may collide. A simple solution to this problem is to
add a ‘namespace’ prefix to the names of all tables ‘owned’ by a specific
application, e.g. for a project ``myproject`` all tables have names like
``myproject_XYZ``.

Instead of adding this application-specific prefix each time a table name is
passed to AIOHappyBase, the `table_prefix` argument to :py:class:`Connection`
can take care of this. AIOHappyBase will prepend that prefix (and an
underscore) to each table name handled by that :py:class:`Connection` instance.
For example::

    connection = Connection('somehost', table_prefix='myproject')

At this point, :py:meth:`Connection.tables` no longer includes tables in other
‘namespaces’. AIOHappyBase will only return tables with a ``myproject_`` prefix,
and will also remove the prefix transparently when returning results, e.g.::

    print(await connection.tables())  # Table "myproject_XYZ" in HBase will be
                                      # returned as simply "XYZ"

This also applies to other methods that take table names, such as
:py:meth:`Connection.table`::

   table = connection.table('XYZ')  # Operates on myproject_XYZ in HBase

The end result is that the table prefix is specified only once in your code,
namely in the call to the :py:class:`Connection` constructor, and that only a
single change is necessary in case it needs changing.


Retrieving data
===============

The HBase data model is a multidimensional sparse map. A table in HBase
contains column families with column qualifiers containing a value and a
timestamp. In most of the AIOHappyBase API, column family and qualifier names
are specified as a single string, e.g. ``cf1:col1``, and not as two separate
arguments. While column families and qualifiers are different concepts in the
HBase data model, they are almost always used together when interacting with
data, so treating them as a single string makes the API a lot simpler.

Retrieving rows
---------------

The :py:class:`Table` class offers various methods to retrieve data from a
table in HBase. The most basic one is :py:meth:`Table.row`, which retrieves a
single row from the table, and returns it as a dictionary mapping columns to
values::

   row = await table.row(b'row-key')
   print(row[b'cf1:col1'])   # prints the value of cf1:col1

The :py:meth:`Table.rows` method works just like :py:meth:`Table.row`, but
takes multiple row keys and returns those as `(key, data)` tuples::

   rows = await table.rows([b'row-key-1', b'row-key-2'])
   for key, data in rows:
       print(key, data)

If you want the results that :py:meth:`Table.rows` returns as a dictionary,
you will have to do this yourself. This is really easy though, since the return
value can be passed directly to the dictionary constructor. As of Python 3.6
order is not lost when creating a dictionary::

    rows_as_dict = dict(await table.rows([b'row-key-1', b'row-key-2']))


Making more fine-grained selections
-----------------------------------

HBase's data model allows for more fine-grained selections of the data to
retrieve. If you know beforehand which columns are needed, performance can be
improved by specifying those columns explicitly to :py:meth:`Table.row` and
:py:meth:`Table.rows`. The `columns` argument takes a list (or tuple) of column
names::

   row = await table.row(b'row-key', columns=[b'cf1:col1', b'cf1:col2'])
   print(row[b'cf1:col1'])
   print(row[b'cf1:col2'])

Instead of providing both a column family and a column qualifier, items in the
`columns` argument may also be just a column family, which means that all
columns from that column family will be retrieved. For example, to get all
columns and values in the column family `cf1`, use this::

   row = await table.row(b'row-key', columns=[b'cf1'])

In HBase, each cell has a timestamp attached to it. In case you don't want to
work with the latest version of data stored in HBase, the methods that retrieve
data from the database, e.g. :py:meth:`Table.row`, all accept a `timestamp`
argument that specifies that the results should be restricted to values with a
timestamp up to the specified timestamp::

    row = await table.row(b'row-key', timestamp=123456789)

By default, AIOHappyBase does not include timestamps in the results it returns.
In your application needs access to the timestamps, simply set the
`include_timestamp` argument to ``True``. Now, each cell in the result will be
returned as a `(value, timestamp)` tuple instead of just a value::

    row = await table.row(b'row-key', columns=[b'cf1:col1'], include_timestamp=True)
    value, timestamp = row[b'cf1:col1']

HBase supports storing multiple versions of the same cell. This can be
configured for each column family. To retrieve all versions of a column for a
given row, :py:meth:`Table.cells` can be used. This method returns an ordered
list of cells, with the most recent version coming first. The `versions`
argument specifies the maximum number of versions to return. Just like the
methods that retrieve rows, the `include_timestamp` argument determines whether
timestamps are included in the result. Example::

    values = await table.cells(b'row-key', b'cf1:col1', versions=2)
    for value in values:
        print("Cell data: {}".format(value))

    cells = await table.cells(b'row-key', b'cf1:col1', versions=3, include_timestamp=True)
    for value, timestamp in cells:
        print("Cell data at {}: {}".format(timestamp, value))

Note that the result may contain fewer cells than requested. The cell may just
have fewer versions, or you may have requested more versions than HBase keeps
for the column family.

Scanning over rows in a table
-----------------------------

In addition to retrieving data for known row keys, rows in HBase can be
efficiently iterated over using a table scanner, created using
:py:meth:`Table.scan`. A basic scanner that iterates over all rows in the table
looks like this::

    async for key, data in table.scan():
        print(key, data)

Doing full table scans like in the example above is prohibitively expensive in
practice. Scans can be restricted in several ways to make more selective range
queries. One way is to specify start or stop keys, or both. To iterate over all
rows from row `aaa` to the end of the table::

    async for key, data in table.scan(row_start=b'aaa'):
        print(key, data)

To iterate over all rows from the start of the table up to row `xyz`, use this::

    async for key, data in table.scan(row_stop=b'xyz'):
        print(key, data)

To iterate over all rows between row `aaa` (included) and `xyz` (not included),
supply both::

    async for key, data in table.scan(row_start=b'aaa', row_stop=b'xyz'):
        print(key, data)

An alternative is to use a key prefix. For example, to iterate over all rows
starting with `abc`::

    async for key, data in table.scan(row_prefix=b'abc'):
        print(key, data)

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

HBase does not have any notion of *data types*; all row keys, column
names and column values are simply treated as raw byte strings.

By design, AIOHappyBase does *not* do any automatic string conversion.
This means that data must be converted to byte strings in your
application before you pass it to AIOHappyBase, for instance by calling
``s.encode('utf-8')`` on text strings (which use Unicode), or by
employing more advanced string serialisation techniques like
``struct.pack()``. Look for HBase modelling techniques for more
details about this. Note that the underlying Thrift library used by
AIOHappyBase does some automatic encoding of text strings into bytes, but
relying on this "feature" is strongly discouraged, since returned data
will not be decoded automatically, resulting in asymmetric and hence
confusing behaviour. Having explicit encode and decode steps in your
application code is the correct way.

In HBase, all mutations either store data or mark data for deletion; there is
no such thing as an in-place `update` or `delete`.  AIOHappyBase provides
methods to do single inserts or deletes, and a batch API to perform multiple
mutations in one go.

Storing data
------------

To store a single cell of data in our table, we can use :py:meth:`Table.put`,
which takes the row key, and the data to store. The data should be a dictionary
mapping the column name to a value::

    await table.put(b'row-key', {b'cf:col1': b'value1', b'cf:col2': b'value2'})

Use the `timestamp` argument if you want to provide timestamps explicitly::

    await table.put(b'row-key', {b'cf:col1': b'value1'}, timestamp=123456789)

If omitted, HBase defaults to the current system time.

Deleting data
-------------

The :py:meth:`Table.delete` method deletes data from a table. To delete a
complete row, just specify the row key::

    await table.delete(b'row-key')

To delete one or more columns instead of a complete row, also specify the
`columns` argument::

    await table.delete(b'row-key', columns=[b'cf1:col1', b'cf1:col2'])

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
    await b.put(b'row-key-1', {b'cf:col1': b'value1', b'cf:col2': b'value2'})
    await b.put(b'row-key-2', {b'cf:col2': b'value2', b'cf:col3': b'value3'})
    await b.put(b'row-key-3', {b'cf:col3': b'value3', b'cf:col4': b'value4'})
    await b.delete(b'row-key-4')
    await b.send()

.. note::

   Storing and deleting data for the same row key in a single batch leads to
   unpredictable results, so don't do that.

While the methods on the :py:class:`Batch` instance resemble the
:py:meth:`~Table.put` and :py:meth:`~Table.delete` methods, they do not take a
`timestamp` argument for each mutation. Instead, you can specify a single
`timestamp` argument for the complete batch::

    b = table.batch(timestamp=123456789)
    await b.put(...)
    await b.delete(...)
    await b.send()

:py:class:`Batch` instances can be used as *context managers*, which are most
useful in combination with Python's ``with`` construct. The example above can
be simplified to read::

    async with table.batch() as b:
        await b.put(b'row-key-1', {b'cf:col1': b'value1', b'cf:col2': b'value2'})
        await b.put(b'row-key-2', {b'cf:col2': b'value2', b'cf:col3': b'value3'})
        await b.put(b'row-key-3', {b'cf:col3': b'value3', b'cf:col4': b'value4'})
        await b.delete(b'row-key-4')

As you can see, there is no call to :py:meth:`Batch.send` anymore. The batch is
automatically applied when the ``with`` code block terminates, even in case of
errors somewhere in the ``with`` block, so it behaves basically the same as a
``try/finally`` clause. However, some applications require transactional
behaviour, sending the batch only if no exception occurred. Without a context
manager this would look something like this::

    b = table.batch()
    try:
         await b.put(b'row-key-1', {b'cf:col1': b'value1', b'cf:col2': b'value2'})
         await b.put(b'row-key-2', {b'cf:col2': b'value2', b'cf:col3': b'value3'})
         await b.put(b'row-key-3', {b'cf:col3': b'value3', b'cf:col4': b'value4'})
         await b.delete(b'row-key-4')
         raise ValueError("Something went wrong!")
    except ValueError as e:
         # error handling goes here; nothing will be sent to HBase
         pass
    else:
         # no exceptions; send data
         await b.send()

Obtaining the same behaviour is easier using a ``with`` block. The
`transaction` argument to :py:meth:`Table.batch` is all you need::

    try:
         async with table.batch(transaction=True) as b:
              await b.put(b'row-key-1', {b'cf:col1': b'value1', b'cf:col2': b'value2'})
              await b.put(b'row-key-2', {b'cf:col2': b'value2', b'cf:col3': b'value3'})
              await b.put(b'row-key-3', {b'cf:col3': b'value3', b'cf:col4': b'value4'})
              await b.delete(b'row-key-4')
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

    async with table.batch(batch_size=1000) as b:
         for i in range(1200):
              # this put() will result in two mutations (two cells)
              await b.put(b'row-%04d' % i, {
                    b'cf1:col1': b'v1',
                    b'cf1:col2': b'v2',
              })

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

    print(await table.counter_inc(b'row-key', b'cf1:counter'))  # prints 1
    print(await table.counter_inc(b'row-key', b'cf1:counter'))  # prints 2
    print(await table.counter_inc(b'row-key', b'cf1:counter'))  # prints 3

    print(await table.counter_dec(b'row-key', b'cf1:counter'))  # prints 2

The optional `value` argument specifies how much to increment or decrement by::

    print(await table.counter_inc(b'row-key', b'cf1:counter', value=3))  # prints 5

While counters are typically used with the increment and decrement functions
shown above, the :py:meth:`Table.counter_get` and :py:meth:`Table.counter_set`
methods can be used to retrieve or set a counter value directly::

    print(await table.counter_get(b'row-key', b'cf1:counter'))  # prints 5

    await table.counter_set(b'row-key', b'cf1:counter', 12)

.. note::

   An application should *never* :py:meth:`~Table.counter_get` the current
   value, modify it in code and then :py:meth:`~Table.counter_set` the modified
   value; use the atomic :py:meth:`~Table.counter_inc` and
   :py:meth:`~Table.counter_dec` instead!


Using the connection pool
=========================

AIOHappyBase comes with a asyncio task-safe connection pool that allows
multiple tasks to share and reuse open connections. This is most useful in
multi-tasked server applications such as ``aiohttp`` servers. When a task asks
the pool for a connection (using :py:meth:`ConnectionPool.connection`), it will
be granted a lease, during which the task has exclusive access to the
connection. After the task is done using the connection, it returns the
connection to the pool so that it becomes available for other tasks.

Instantiating the pool
----------------------

The pool is provided by the :py:class:`ConnectionPool` class. The `size`
argument to the constructor specifies the number of connections in the pool.
Additional arguments are passed on to the :py:class:`Connection` constructor::

    from aiohappybase import ConnectionPool

    pool = ConnectionPool(size=3, host='...', table_prefix='myproject')

    # Context instantiation is preferred to make sure connections are cleaned up
    async with ConnectionPool(size=3, host='...', table_prefix='myproject') as pool:
        # Do your thing

Connections will only be opened as necessary. HappyBase's ConnectionPool
would open a single connection immediately to detect issues, but that isn't
so easy to do in async because __init__ is always synchronous.

Obtaining connections
---------------------

Connections can only be obtained using Python's context manager protocol, i.e.
using a code block inside a ``with`` statement. This ensures that connections
are actually returned to the pool after use. Example::

    async with .ConnectionPool(size=3, host='...') as pool:
        async with pool.connection() as connection:
             print(await connection.tables())

.. warning::

   Never use the ``connection`` instance after the ``with`` block has ended.
   Even though the variable is still in scope, the connection may have been
   assigned to another task in the mean time.

Connections should be returned to the pool as quickly as possible, so that other
tasks can use them. This means that the amount of code included inside the
``with`` block should be kept to an absolute minimum. In practice, an
application should only load data inside the ``with`` block, and process the
data outside the ``with`` block::

    async with pool.connection() as connection:
         table = connection.table('table-name')
         row = await table.row(b'row-key')

    process_data(row)

An application task can only hold one connection at a time. When a task
holds a connection and asks for a connection for a second time (e.g. because a
called function also requests a connection from the pool), the same connection
instance it already holds is returned, so this does not require any coordination
from the application. This means that in the following example, both connection
requests to the pool will return the exact same connection::

    pool = ConnectionPool(size=3, host='...')

    async def do_something_else():
         async with pool.connection() as connection:
              pass  # use the connection here

    async with pool.connection() as connection:
         # use the connection here, e.g.
         print(await connection.tables())

         # call another function that uses a connection
         do_something_else()

    await pool.close()

Handling broken connections
---------------------------

The pool tries to detect broken connections and will replace those with fresh
ones when the connection is returned to the pool. However, the connection pool
does not capture raised exceptions, nor does it automatically retry failed
operations. This means that the application still has to handle connection
errors.


.. rubric:: Next steps

The next step is to try it out for yourself! The :doc:`API documentation <api>`
can be used as a reference.

.. vim: set spell spelllang=en:
