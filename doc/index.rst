=========
HappyBase
=========

.. py:currentmodule:: happybase

**HappyBase** is a developer-friendly Python__ library to interact with `Apache
HBase`__. HappyBase is designed for use in standard HBase setups, and offers
application developers a Pythonic API to interact with HBase. Below the surface,
HappyBase uses the `Python Thrift library`__ to connect to HBase using its
Thrift__ gateway, which is included in the standard HBase 0.9x releases.

__ http://python.org/
__ http://hbase.apache.org/
__ http://pypi.python.org/pypi/thrift
__ http://thrift.apache.org/


.. note::

   **Do you enjoy HappyBase?** Great! You should know that I don't use HappyBase
   myself anymore, but still maintain it because it's quite popular. Please
   consider making a small donation__ to let me know you appreciate my work.
   Thanks!

   __ https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=ZJ9U8DNN6KZ9Q


Example
=======

The example below illustrates basic usage of the library. The :doc:`user guide
<user>` contains many more examples.

::

   import happybase

   connection = happybase.Connection('hostname')
   table = connection.table('table-name')

   table.put(b'row-key', {b'family:qual1': b'value1',
                          b'family:qual2': b'value2'})

   row = table.row(b'row-key')
   print(row[b'family:qual1'])  # prints 'value1'

   for key, data in table.rows([b'row-key-1', b'row-key-2']):
       print(key, data)  # prints row key and data for each row

   for key, data in table.scan(row_prefix=b'row'):
       print(key, data)  # prints 'value1' and 'value2'

   row = table.delete(b'row-key')


Core documentation
==================

.. toctree::
   :maxdepth: 2

   installation
   user
   api


Additional documentation
========================

.. toctree::
   :maxdepth: 1

   news
   development
   todo
   faq
   license


External links
==============

* `Online documentation <https://happybase.readthedocs.io/>`_ (Read the Docs)
* `Downloads <http://pypi.python.org/pypi/happybase/>`_ (PyPI)
* `Source code <https://github.com/wbolster/happybase>`_ (Github)


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. vim: set spell spelllang=en:
