************
Introduction
************

.. py:currentmodule:: happybase

**HappyBase** is a developer-friendly `Python <http://python.org/>`_ library to
interact with `Apache HBase <http://hbase.apache.org/>`_. HappyBase is designed
for use in standard HBase setups, and offers application developers a Pythonic
API to interact with HBase.

The example below illustrates basic usage of the library. The :doc:`tutorial
<tutorial>` contains many more examples.

::

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

Below the surface, HappyBase uses the `Python Thrift library
<http://pypi.python.org/pypi/thrift>`_ to connect to HBase using its `Thrift
<http://thrift.apache.org/>`_ gateway, which is included in the standard HBase
0.9x releases.


.. rubric:: Next steps

Follow the :doc:`installation guide <installation>` and continue with the
:doc:`tutorial <tutorial>`.


.. vim: set spell spelllang=en:
