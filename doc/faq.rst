==========================
Frequently asked questions
==========================


I love AIOHappyBase! Can I donate?
==================================

While I am not accepting donations at this time, the original author is:

**From the original HappyBase author, Wouter Bolsterlee:**

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

    import asyncio as aio

    from thriftpy2.contrib.aio.client import TAsyncClient
    from thriftpy2.contrib.aio.socket import TAsyncSocket
    from thriftpy2.contrib.aio.transport.buffered import TAsyncBufferedTransport
    from thriftpy2.contrib.aio.protocol.binary import TAsyncBinaryProtocol

    from hbase import Hbase, Mutation

    async def main():

        sock = TAsyncSocket('hostname', 9090)
        transport = TAsyncBufferedTransport(sock)
        protocol = TAsyncBinaryProtocol(transport)
        client = TAsyncClient(Hbase, protocol)
        transport.open()

        mutations = [
            Mutation(column=b'family:qual1', value=b'value1'),
            Mutation(column=b'family:qual2', value=b'value2'),
        ]
        await client.mutateRow(b'table-name', b'row-key', mutations)

    aio.run(main())


:pep:`20` taught us that simple is better than complex, and as you can see,
Thrift is certainly complex. AIOHappyBase hides all the Thrift cruft below a
friendly API. The resulting application code will be cleaner, more productive
to write, and more maintainable. With AIOHappyBase, the example above can be
simplified to this::

    import asyncio as aio

    from aiohappybase import Connection

    async def main():
        async with Connection('hostname') as conn:
            table = conn.table(b'table-name')
            await table.put(b'row-key', {
                 b'family:qual1': b'value1',
                 b'family:qual2': b'value2',
            })

    aio.run(main())

If you're not convinced and still think the Thrift API is not that bad, please
try to accomplish some other common tasks, e.g. retrieving rows and scanning
over a part of a table, and compare that to the AIOHappyBase equivalents. If
you're still not convinced by then, we're sorry to inform you that AIOHappyBase
is not the project for you, and we wish you all of luck maintaining your code
‒ or is it just Thrift boilerplate?
