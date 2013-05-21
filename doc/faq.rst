==========================
Frequently asked questions
==========================


I love HappyBase! Can I donate?
===============================

Thanks, I'm glad to hear that you appreciate my work! If you feel like, please
make a small donation_ to sponsor my (spare time!) work on HappyBase. Small
gestures are really motivating for me and help me keep this project going!

.. _donation: https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=ZJ9U8DNN6KZ9Q


Why not use the Thrift API directly?
====================================

While the HBase Thrift API can be used directly from Python using (automatically
generated) HBase Thrift service classes, application code doing so is very
verbose, cumbersome to write, and hence error-prone. The reason for this is that
the HBase Thrift API is a flat, language-agnostic interface API closely tied to
the RPC going over the wire-level protocol. In practice, this means that
applications using Thrift directly need to deal with many imports, sockets,
transports, protocols, clients, Thrift types and mutation objects. For instance,
look at the code required to connect to HBase and store two values::

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

:pep:`20` taught us that simple is better than complex, and as you can see,
Thrift is certainly complex. HappyBase hides all the Thrift cruft below a
friendly API. The resulting application code will be cleaner, more productive
to write, and more maintainable. With HappyBase, the example above can be
simplified to this::

   import happybase

   connection = happybase.Connection('hostname')
   table = connection.table('table-name')
   table.put('row-key', {'family:qual1': 'value1',
                         'family:qual2': 'value2'})

If you're not convinced and still think the Thrift API is not that bad, please
try to accomplish some other common tasks, e.g. retrieving rows and scanning
over a part of a table, and compare that to the HappyBase equivalents. If
you're still not convinced by then, we're sorry to inform you that HappyBase is
not the project for you, and we wish you all of luck maintaining your code ‒ or
is it just Thrift boilerplate?
