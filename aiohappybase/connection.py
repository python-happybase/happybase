"""
AIOHappyBase connection module.
"""

import logging
import asyncio as aio
import inspect
from typing import AnyStr, List, Dict, Any

from thriftpy2.contrib.aio.protocol.binary import TAsyncBinaryProtocol
from thriftpy2.contrib.aio.transport.buffered import TAsyncBufferedTransport
from thriftpy2.contrib.aio.socket import TAsyncSocket
from thriftpy2.contrib.aio.client import TAsyncClient

from Hbase_thrift import Hbase, ColumnDescriptor

from .table import Table
from ._util import ensure_bytes, pep8_to_camel_case

logger = logging.getLogger(__name__)

STRING_OR_BINARY = (str, bytes)

COMPAT_MODES = ('0.90', '0.92', '0.94', '0.96', '0.98')

# TODO: Auto generate these?
THRIFT_TRANSPORTS = dict(
    buffered=TAsyncBufferedTransport,
)
THRIFT_PROTOCOLS = dict(
    binary=TAsyncBinaryProtocol,
)

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090
DEFAULT_TRANSPORT = 'buffered'
DEFAULT_COMPAT = '0.98'
DEFAULT_PROTOCOL = 'binary'


class Connection:
    """
    Connection to an HBase Thrift server.

    The `host` and `port` arguments specify the host name and TCP port
    of the HBase Thrift server to connect to. If omitted or ``None``,
    a connection to the default port on ``localhost`` is made. If
    specifed, the `timeout` argument specifies the socket timeout in
    milliseconds.

    If `autoconnect` is `True` (the default) the connection is made
    directly, otherwise :py:meth:`Connection.open` must be called
    explicitly before first use.

    The optional `table_prefix` and `table_prefix_separator` arguments
    specify a prefix and a separator string to be prepended to all table
    names, e.g. when :py:meth:`Connection.table` is invoked. For
    example, if `table_prefix` is ``myproject``, all tables will
    have names like ``myproject_XYZ``.

    The optional `compat` argument sets the compatibility level for
    this connection. Older HBase versions have slightly different Thrift
    interfaces, and using the wrong protocol can lead to crashes caused
    by communication errors, so make sure to use the correct one. This
    value can be either the string ``0.90``, ``0.92``, ``0.94``, or
    ``0.96`` (the default).

    The optional `transport` argument specifies the Thrift transport
    mode to use. Supported values for this argument are ``buffered``
    (the default) and ``framed``. Make sure to choose the right one,
    since otherwise you might see non-obvious connection errors or
    program hangs when making a connection. HBase versions before 0.94
    always use the buffered transport. Starting with HBase 0.94, the
    Thrift server optionally uses a framed transport, depending on the
    argument passed to the ``hbase-daemon.sh start thrift`` command.
    The default ``-threadpool`` mode uses the buffered transport; the
    ``-hsha``, ``-nonblocking``, and ``-threadedselector`` modes use the
    framed transport.

    The optional `protocol` argument specifies the Thrift transport
    protocol to use. Supported values for this argument are ``binary``
    (the default) and ``compact``. Make sure to choose the right one,
    since otherwise you might see non-obvious connection errors or
    program hangs when making a connection. ``TCompactProtocol`` is
    a more compact binary format that is  typically more efficient to
    process as well. ``TBinaryProtocol`` is the default protocol that
    Happybase uses.

    .. versionadded:: 0.9
       `protocol` argument

    .. versionadded:: 0.5
       `timeout` argument

    .. versionadded:: 0.4
       `table_prefix_separator` argument

    .. versionadded:: 0.4
       support for framed Thrift transports

    :param host: The host to connect to
    :param port: The port to connect to
    :param timeout: The socket timeout in milliseconds (optional)
    :param autoconnect: Whether the connection should be opened directly
    :param table_prefix: Prefix used to construct table names (optional)
    :param table_prefix_separator: Separator used for `table_prefix`
    :param compat: Compatibility mode (optional)
    :param transport: Thrift transport mode (optional)
    :param protocol: Thrift protocol mode (optional)
    """
    def __init__(self,
                 host: str = DEFAULT_HOST,
                 port: int = DEFAULT_PORT,
                 timeout: int = None,
                 autoconnect: bool = False,
                 table_prefix: str = None,
                 table_prefix_separator: bytes = b'_',
                 compat: str = DEFAULT_COMPAT,
                 transport: str = DEFAULT_TRANSPORT,
                 protocol: str = DEFAULT_PROTOCOL):

        if transport not in THRIFT_TRANSPORTS:
            raise ValueError(f"'transport' not in {list(THRIFT_TRANSPORTS)}")

        if table_prefix is not None:
            if not isinstance(table_prefix, STRING_OR_BINARY):
                raise TypeError("'table_prefix' must be a string")
            table_prefix = ensure_bytes(table_prefix)

        if not isinstance(table_prefix_separator, STRING_OR_BINARY):
            raise TypeError("'table_prefix_separator' must be a string")
        table_prefix_separator = ensure_bytes(table_prefix_separator)

        if compat not in COMPAT_MODES:
            raise ValueError(f"'compat' not in {list(COMPAT_MODES)}")

        if protocol not in THRIFT_PROTOCOLS:
            raise ValueError(f"'protocol' not in {list(THRIFT_PROTOCOLS)}")

        # Allow host and port to be None, which may be easier for
        # applications wrapping a Connection instance.
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT
        self.timeout = timeout
        self.table_prefix = table_prefix
        self.table_prefix_separator = table_prefix_separator
        self.compat = compat

        self._transport_class = THRIFT_TRANSPORTS[transport]
        self._protocol_class = THRIFT_PROTOCOLS[protocol]

        self._refresh_thrift_client()

        if autoconnect:
            loop = aio.get_event_loop()
            if loop.is_running():
                raise RuntimeError(
                    "'autoconnect' cannot be used inside a running event loop!"
                )
            else:
                loop.run_until_complete(self.open())

        self._initialized = True

    def _refresh_thrift_client(self) -> None:
        """Refresh the Thrift socket, transport, and client."""
        # TODO: Support all kwargs to make_client
        socket = TAsyncSocket(self.host, self.port, socket_timeout=self.timeout)
        self.transport = self._transport_class(socket)
        protocol = self._protocol_class(self.transport, decode_response=False)
        self.client = TAsyncClient(Hbase, protocol)

    def _table_name(self, name: AnyStr) -> bytes:
        """Construct a table name by optionally adding a table name prefix."""
        name = ensure_bytes(name)
        if self.table_prefix is None:
            return name
        return self.table_prefix + self.table_prefix_separator + name

    async def open(self) -> None:
        """Open the underlying transport to the HBase instance.

        This method opens the underlying Thrift transport (TCP connection).
        """
        if self.transport.is_open():
            return

        logger.debug(f"Opening Thrift transport to {self.host}:{self.port}")
        await self.transport.open()

    async def close(self) -> None:
        """
        Close the underlying transport to the HBase instance.

        This method closes the underlying Thrift transport (TCP connection).
        """
        if not self.transport.is_open():
            return

        if logger is not None:
            # If called from __del__(), module variables may no longer exist.
            logger.debug(f"Closing Thrift transport to {self.host}:{self.port}")

        closer = self.transport.close()
        if inspect.isawaitable(closer):  # Allow async close methods
            await closer
        # Socket isn't really closed yet, wait for it
        await aio.sleep(0)

    def table(self, name: AnyStr, use_prefix: bool = True) -> Table:
        """
        Return a table object.

        Returns a :py:class:`happybase.Table` instance for the table
        named `name`. This does not result in a round-trip to the
        server, and the table is not checked for existence.

        The optional `use_prefix` argument specifies whether the table
        prefix (if any) is prepended to the specified `name`. Set this
        to `False` if you want to use a table that resides in another
        ‘prefix namespace’, e.g. a table from a ‘friendly’ application
        co-hosted on the same HBase instance. See the `table_prefix`
        argument to the :py:class:`Connection` constructor for more
        information.

        :param name: the name of the table
        :param use_prefix: whether to use the table prefix (if any)
        :return: Table instance
        """
        name = ensure_bytes(name)
        if use_prefix:
            name = self._table_name(name)
        return Table(name, self)

    # Table administration and maintenance

    async def tables(self) -> List[bytes]:
        """
        Return a list of table names available in this HBase instance.

        If a `table_prefix` was set for this :py:class:`Connection`, only
        tables that have the specified prefix will be listed.

        :return: The table names
        """
        names = await self.client.getTableNames()

        # Filter using prefix, and strip prefix from names
        if self.table_prefix is not None:
            prefix = self._table_name(b'')
            offset = len(prefix)
            names = [n[offset:] for n in names if n.startswith(prefix)]

        return names

    async def create_table(self,
                           name: AnyStr,
                           families: Dict[str, Dict[str, Any]]) -> Table:
        """
        Create a table.

        :param name: The table name
        :param families: The name and options for each column family
        :return: The created table instance

        The `families` argument is a dictionary mapping column family
        names to a dictionary containing the options for this column
        family, e.g.

        ::

            families = {
                'cf1': dict(max_versions=10),
                'cf2': dict(max_versions=1, block_cache_enabled=False),
                'cf3': dict(),  # use defaults
            }
            connection.create_table('mytable', families)

        These options correspond to the ColumnDescriptor structure in
        the Thrift API, but note that the names should be provided in
        Python style, not in camel case notation, e.g. `time_to_live`,
        not `timeToLive`. The following options are supported:

        * ``max_versions`` (`int`)
        * ``compression`` (`str`)
        * ``in_memory`` (`bool`)
        * ``bloom_filter_type`` (`str`)
        * ``bloom_filter_vector_size`` (`int`)
        * ``bloom_filter_nb_hashes`` (`int`)
        * ``block_cache_enabled`` (`bool`)
        * ``time_to_live`` (`int`)
        """
        name = self._table_name(name)
        if not isinstance(families, dict):
            raise TypeError("'families' arg must be a dictionary")

        if not families:
            raise ValueError(f"No column families given for table: {name!r}")

        column_descriptors = []
        for cf_name, options in families.items():
            kwargs = {
                pep8_to_camel_case(option_name): value
                for option_name, value in (options or {}).items()
            }

            if not cf_name.endswith(':'):
                cf_name += ':'
            kwargs['name'] = cf_name

            column_descriptors.append(ColumnDescriptor(**kwargs))

        await self.client.createTable(name, column_descriptors)
        return self.table(name, use_prefix=False)

    async def delete_table(self, name: AnyStr, disable: bool = False) -> None:
        """
        Delete the specified table.

        .. versionadded:: 0.5
           `disable` argument

        In HBase, a table always needs to be disabled before it can be
        deleted. If the `disable` argument is `True`, this method first
        disables the table if it wasn't already and then deletes it.

        :param name: The table name
        :param disable: Whether to first disable the table if needed
        """
        if disable and await self.is_table_enabled(name):
            await self.disable_table(name)

        name = self._table_name(name)
        await self.client.deleteTable(name)

    async def enable_table(self, name: AnyStr) -> None:
        """
        Enable the specified table.

        :param name: The table name
        """
        name = self._table_name(name)
        await self.client.enableTable(name)

    async def disable_table(self, name: AnyStr) -> None:
        """
        Disable the specified table.

        :param name: The table name
        """
        name = self._table_name(name)
        await self.client.disableTable(name)

    async def is_table_enabled(self, name: AnyStr) -> None:
        """
        Return whether the specified table is enabled.

        :param str name: The table name

        :return: whether the table is enabled
        :rtype: bool
        """
        name = self._table_name(name)
        return await self.client.isTableEnabled(name)

    async def compact_table(self, name: AnyStr, major: bool = False) -> None:
        """Compact the specified table.

        :param str name: The table name
        :param bool major: Whether to perform a major compaction.
        """
        name = self._table_name(name)
        if major:
            await self.client.majorCompact(name)
        else:
            await self.client.compact(name)

    # Support async context usage
    async def __aenter__(self) -> 'Connection':
        await self.open()
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.close()

    # Support context usage
    def __enter__(self) -> 'Connection':
        try:
            aio.get_event_loop().run_until_complete(self.open())
        except RuntimeError:
            raise RuntimeError("Use async with inside a running event loop!")
        return self

    def __exit__(self, *_exc) -> None:
        aio.get_event_loop().run_until_complete(self.close())

    def __del__(self) -> None:
        try:
            if self.transport.is_open():
                logger.warning(f"{self} was not closed!")
        except:  # noqa
            pass
