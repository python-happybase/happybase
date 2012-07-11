# coding: UTF-8

"""
HappyBase main API module.
"""

import logging
logger = logging.getLogger(__name__)

from collections import defaultdict
from operator import attrgetter
from struct import Struct

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TFramedTransport
from thrift.protocol import TBinaryProtocol

from .hbase import Hbase
from .hbase.ttypes import BatchMutation, ColumnDescriptor, Mutation, TScan

from .util import thrift_type_to_dict, pep8_to_camel_case, str_increment

__all__ = ['DEFAULT_HOST', 'DEFAULT_PORT', 'Connection', 'Table', 'Batch']


# TODO: properly handle errors defined in Thrift specification

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090

COMPAT_MODES = ('0.90', '0.92')
THRIFT_TRANSPORTS = {'buffered': TBufferedTransport,
                     'framed': TFramedTransport}

make_cell = attrgetter('value')
make_cell_timestamp = attrgetter('value', 'timestamp')
pack_i64 = Struct('>q').pack


def _make_row(cell_map, include_timestamp):
    """Makes a row dict for a cell mapping like ttypes.TRowResult.columns."""
    cellfn = include_timestamp and make_cell_timestamp or make_cell
    return dict((cn, cellfn(cell)) for cn, cell in cell_map.iteritems())


class Connection(object):
    """Connection to an HBase Thrift server.

    The `host` and `port` parameters specify the host name and TCP port of the
    HBase Thrift server to connect to. If omitted or ``None``, a connection to
    the default port on ``localhost`` is made.

    If `autoconnect` is `True` (the default) the connection is made directly,
    otherwise :py:meth:`Connection.open` must be called explicitly before first
    use.

    The optional `table_prefix` and `table_prefix_separator` arguments specify
    a prefix and a separator string to be prepended to all table names, e.g.
    when :py:meth:`Connection.table` is invoked. For example, if `table_prefix`
    is ``myproject``, all tables tables will have names like ``myproject_XYZ``.

    The optional `compat` parameter sets the compatibility level for this
    connection. Older HBase versions have slightly different Thrift interfaces,
    and using the wrong protocol can lead to crashes caused by communication
    errors, so make sure to use the correct one. This value can be either the
    string ``0.92`` (the default) for use with HBase 0.92.x and later versions,
    or ``0.90`` for use with HBase 0.90.x.

    The optional `transport` parameter specifies the Thrift transport mode to
    use. Supported values for this parameter are ``buffered`` (the default) and
    ``framed``. Make sure to choose the right one, since otherwise you might
    see non-obvious connection errors or program hangs when making
    a connection. HBase versions before 0.94 always use the buffered transport.
    Starting with HBase 0.94, the Thrift server optionally uses a framed
    transport, depending on the parameter passed to the ``hbase-daemon.sh start
    thrift`` command. The default ``-threadpool`` mode uses the buffered
    transport; the ``-hsha``, ``-nonblocking``, and ``-threadedselector`` modes
    use the framed transport.

    :param str host: The host to connect to
    :param int port: The port to connect to
    :param bool autoconnect: Whether the connection should be opened directly.
    :param str table_prefix: Prefix used to construct table names (optional)
    :param str table_prefix_separator: Separator used for `table_prefix`
    :param str compat: Compatibility mode (optional)
    :param str transport: Thrift transport mode (optional)
    """
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, autoconnect=True,
                 table_prefix=None, table_prefix_separator='_', compat='0.92',
                 transport='buffered'):

        # Allow host and port to be None, which may be easier for
        # applications wrapping a Connection instance.
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT

        if compat not in COMPAT_MODES:
            raise ValueError("'compat' must be one of %s"
                             % ", ".join(COMPAT_MODES))

        if transport not in THRIFT_TRANSPORTS:
            raise ValueError("'transport' must be one of %s"
                             % ", ".join(THRIFT_TRANSPORTS.keys()))

        if table_prefix is not None and not isinstance(table_prefix, basestring):
            raise TypeError("'table_prefix' must be a string")

        if not isinstance(table_prefix_separator, basestring):
            raise TypeError("'table_prefix_separator' must be a string")

        self.compat = compat
        self.table_prefix = table_prefix
        self.table_prefix_separator = table_prefix_separator

        socket = TSocket(self.host, self.port)
        self.transport = THRIFT_TRANSPORTS[transport](socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        self.client = Hbase.Client(protocol)

        if autoconnect:
            self.open()

        self._initialized = True

    def _table_name(self, name):
        """Constructs a table name by optionally adding a table name prefix."""
        if self.table_prefix is None:
            return name

        return self.table_prefix + self.table_prefix_separator + name

    def open(self):
        """Opens the underlying transport to the HBase instance.

        This method opens the underlying Thrift transport (TCP connection).
        """
        logger.debug("Opening Thrift transport to %s:%d", self.host, self.port)
        self.transport.open()

    def close(self):
        """Closes the underyling transport to the HBase instance.

        This method closes the underlying Thrift transport (TCP connection).
        """
        logger.debug("Closing Thrift transport to %s:%d", self.host, self.port)
        self.transport.close()

    def __del__(self):
        try:
            self._initialized
        except AttributeError:
            # Failure from constructor
            return
        else:
            self.close()

    def table(self, name, use_prefix=True):
        """Returns a table object.

        Returns a :py:class:`happybase.Table` instance for the table named
        `name`. This does not result in a round-trip to the server, and the
        table is not checked for existence.

        The optional `use_prefix` parameter specifies whether the table prefix
        (if any) is prepended to the specified `name`. Set this to `False` if
        you want to use a table that resides in another ‘prefix namespace’,
        e.g. a table from a ‘friendly’ application co-hosted on the same HBase
        instance. See the `table_prefix` parameter to the
        :py:class:`Connection` constructor for more information.

        :param str name: the name of the table
        :param bool use_prefix: whether to use the table prefix (if any)
        :return: Table instance
        :rtype: :py:class:`Table`
        """
        if use_prefix:
            name = self._table_name(name)
        return Table(name, self)

    #
    # Table administration and maintenance
    #

    def tables(self):
        """Returns a list of table names available in this HBase instance.

        If a `table_prefix` was set for this :py:class:`Connection`, only
        tables that have the specified prefix will be listed.

        :return: The table names
        :rtype: List of strings
        """
        names = self.client.getTableNames()

        # Filter using prefix, and strip prefix from names
        if self.table_prefix is not None:
            prefix = self._table_name('')
            offset = len(prefix)
            names = [n[offset:] for n in names if n.startswith(prefix)]

        return names

    def create_table(self, name, families):
        """Creates a table.

        :param str name: The table name
        :param dict families: The name and options for each column family

        The `families` parameter is a dictionary mapping column family names to
        a dictionary containing the options for this column family. See
        ColumnDescriptor in the Thrift API for the supported options, but note
        that the names should be provided in Python style, and not in camel
        case notation, for example `time_to_live` (not `timeToLive`) and
        `max_versions` (not `maxVersions`).
        """
        name = self._table_name(name)
        if not isinstance(families, dict):
            raise TypeError("'families' arg must be a dictionary")

        if not families:
            raise ValueError(
                "Cannot create table %r (no column families specified)"
                % name)

        column_descriptors = []
        for cf_name, options in families.iteritems():
            if options is None:
                options = dict()

            kwargs = dict()
            for option_name, value in options.iteritems():
                kwargs[pep8_to_camel_case(option_name)] = value

            if not cf_name.endswith(':'):
                cf_name += ':'
            kwargs['name'] = cf_name

            column_descriptors.append(ColumnDescriptor(**kwargs))

        self.client.createTable(name, column_descriptors)

    def delete_table(self, name):
        """Deletes the specified table.

        :param str name: The table name
        """
        name = self._table_name(name)
        self.client.deleteTable(name)

    def enable_table(self, name):
        """Enables the specified table.

        :param str name: The table name
        """
        name = self._table_name(name)
        self.client.enableTable(name)

    def disable_table(self, name):
        """Disables the specified table.

        :param str name: The table name
        """
        name = self._table_name(name)
        self.client.disableTable(name)

    def is_table_enabled(self, name):
        """Returns whether the specified table is enabled.

        :param str name: The table name

        :return: whether the table is enabled
        :rtype: bool
        """
        name = self._table_name(name)
        return self.client.isTableEnabled(name)

    def compact_table(self, name, major=False):
        """Compacts the specified table.

        :param str name: The table name
        :param bool major: Whether to perform a major compaction.
        """
        name = self._table_name(name)
        if major:
            self.client.majorCompact(name)
        else:
            self.client.compact(name)


class Table(object):
    """HBase table abstraction class.

    This class cannot be instantiated directly; use :py:meth:`Connection.table`
    instead.
    """
    def __init__(self, name, connection):
        self.name = name
        self.connection = connection
        self.client = connection.client

    def __repr__(self):
        return '<%s.%s name=%r>' % (__name__,
                                    self.__class__.__name__,
                                    self.name)

    def families(self):
        """Retrieves the column families for this table.

        :return: Mapping from column family name to settings dict
        :rtype: dict
        """
        descriptors = self.client.getColumnDescriptors(self.name)
        families = dict()
        for name, descriptor in descriptors.items():
            name = name[:-1]  # drop trailing ':'
            families[name] = thrift_type_to_dict(descriptor)
        return families

    def _column_family_names(self):
        """Retrieves the column family names for this table (internal use)"""
        return self.client.getColumnDescriptors(self.name).keys()

    def regions(self):
        """Retrieves the regions for this table.

        :return: regions for this table
        :rtype: list of dicts
        """
        regions = self.client.getTableRegions(self.name)
        return map(thrift_type_to_dict, regions)

    #
    # Data retrieval
    #

    def row(self, row, columns=None, timestamp=None, include_timestamp=False):
        """Retrieves a single row of data.

        This method retrieves the row with the row key specified in the `row`
        argument and returns the columns and values for this row as
        a dictionary.

        The `row` argument is the row key of the row. If the `columns` argument
        is specified, only the values for these columns will be returned
        instead of all available columns. The `columns` argument should be
        a list or tuple containing strings. Each name can be a column family,
        such as `cf1` or `cf1:` (the trailing colon is not required), or
        a column family with a qualifier, such as `cf1:col1`.

        If specified, the `timestamp` argument specifies the maximum version
        that results may have. The `include_timestamp` argument specifies
        whether cells are returned as single values or as `(value, timestamp)`
        tuples.

        :param str row: the row key
        :param list_or_tuple columns: list of columns (optional)
        :param int timestamp: timestamp (optional)
        :param bool include_timestamp: whether timestamps are returned

        :return: Mapping of columns (both qualifier and family) to values
        :rtype: dict
        """
        if columns is not None and not isinstance(columns, (tuple, list)):
            raise TypeError("'columns' must be a tuple or list")

        if timestamp is None:
            rows = self.client.getRowWithColumns(self.name, row, columns)
        else:
            if not isinstance(timestamp, int):
                raise TypeError("'timestamp' must be an integer")
            rows = self.client.getRowWithColumnsTs(self.name, row, columns,
                                                   timestamp)

        if not rows:
            return {}

        return _make_row(rows[0].columns, include_timestamp)

    def rows(self, rows, columns=None, timestamp=None,
             include_timestamp=False):
        """Retrieves multiple rows of data.

        This method retrieves the rows with the row keys specified in the
        `rows` argument, which should be should be a list (or tuple) of row
        keys. The return value is a list of `(row_key, data)` tuples.

        The `columns`, `timestamp` and `include_timestamp` arguments behave
        exactly the same as for :py:meth:`row`.

        :param list rows: list of row keys
        :param list_or_tuple columns: list of columns (optional)
        :param int timestamp: timestamp (optional)
        :param bool include_timestamp: whether timestamps are returned

        :return: List of mappings (columns to values)
        :rtype: list of dicts
        """
        if columns is not None and not isinstance(columns, (tuple, list)):
            raise TypeError("'columns' must be a tuple or list")

        if not rows:
            # Avoid round-trip if the result is empty anyway
            return {}

        if timestamp is None:
            results = self.client.getRowsWithColumns(self.name, rows, columns)
        else:
            if not isinstance(timestamp, int):
                raise TypeError("'timestamp' must be an integer")

            # Work-around a bug in the HBase Thrift server where the
            # timestamp is only applied if columns are specified, at
            # the cost of an extra round-trip.
            if columns is None:
                columns = self._column_family_names()

            results = self.client.getRowsWithColumnsTs(self.name, rows,
                                                       columns, timestamp)

        return [(r.row, _make_row(r.columns, include_timestamp))
                for r in results]

    def cells(self, row, column, versions=None, timestamp=None,
              include_timestamp=False):
        """Retrieves multiple versions of a single cell from the table.

        This method retrieves multiple versions of a cell (if any).

        The `versions` argument defines how many cell versions to
        retrieve at most.

        The `timestamp` and `include_timestamp` arguments behave exactly the
        same as for :py:meth:`row`.

        :param str row: the row key
        :param str column: the column name
        :param int versions: the maximum number of versions to retrieve
        :param int timestamp: timestamp (optional)
        :param bool include_timestamp: whether timestamps are returned

        :return: cell values
        :rtype: list of values
        """
        if versions is None:
            versions = (2 ** 31) - 1  # Thrift type is i32
        elif not isinstance(versions, int):
            raise TypeError("'versions' parameter must be a number or None")
        elif versions < 1:
            raise ValueError("'versions' parameter must be at least 1 (or None)")

        if timestamp is None:
            cells = self.client.getVer(self.name, row, column, versions)
        else:
            if not isinstance(timestamp, int):
                raise TypeError("'timestamp' must be an integer")
            cells = self.client.getVerTs(self.name, row, column, timestamp,
                                         versions)

        if include_timestamp:
            return map(make_cell_timestamp, cells)
        else:
            return map(make_cell, cells)

    def scan(self, row_start=None, row_stop=None, row_prefix=None,
             columns=None, filter=None, timestamp=None,
             include_timestamp=False, batch_size=1000, limit=None):
        """Creates a scanner for data in the table.

        This method returns an iterable that can be used for looping over the
        matching rows. Scanners can be created in two ways:

        * The `row_start` and `row_stop` arguments specify the row keys where
          the scanner should start and stop. It does not matter whether the
          table contains any rows with the specified keys: the first row after
          `row_start` will be the first result, and the last row before
          `row_stop` will be the last result. Note that the start of the range
          is inclusive, while the end is exclusive.

          Both `row_start` and `row_stop` can be `None` to specify the start
          and the end of the table respectively. If both are omitted, a full
          table scan is done. Note that this usually results in severe
          performance problems.

        * Alternatively, if `row_prefix` is specified, only rows with row keys
          matching the prefix will be returned. If given, `row_start` and
          `row_stop` cannot be used.

        The `columns`, `timestamp` and `include_timestamp` arguments behave
        exactly the same as for :py:meth:`row`.

        The `filter` argument may be a filter string that will be applied at
        the server by the region servers.

        If `limit` is given, at most `limit` results will be returned.

        The `batch_size` argument specified how many results should be
        retrieved per batch when retrieving results from the scanner. Only set
        this to a low value (or even 1) if your data is large, since a low
        batch size results in added round-trips to the server.

        **Compatibility note:** The `filter` argument is only available when
        using HBase 0.92 (or up). In HBase 0.90 compatibility mode, specifying
        a `filter` raises an exception.

        :param str row_start: the row key to start at (inclusive)
        :param str row_stop: the row key to stop at (exclusive)
        :param str row_prefix: a prefix of the row key that must match
        :param list_or_tuple columns: list of columns (optional)
        :param str filter: a filter string (optional)
        :param int timestamp: timestamp (optional)
        :param bool include_timestamp: whether timestamps are returned
        :param int batch_size: batch size for retrieving resuls

        :return: generator yielding the rows matching the scan
        :rtype: iterable of `(row_key, row_data)` tuples
        """
        if batch_size < 1:
            raise ValueError("'batch_size' must be >= 1")

        if limit is not None and limit < 1:
            raise ValueError("'limit' must be >= 1")

        if row_prefix is not None:
            if row_start is not None or row_stop is not None:
                raise TypeError("'row_prefix' cannot be combined with 'row_start' or 'row_stop'")

            row_start = row_prefix
            row_stop = str_increment(row_prefix)

        if row_start is None:
            row_start = ''

        client = self.client
        if self.connection.compat == '0.90':
            # The scannerOpenWithScan() Thrift function is not
            # available, so work around it as much as possible with the
            # other scannerOpen*() Thrift functions

            if filter is not None:
                raise NotImplementedError("'filter' is not supported in HBase 0.90")

            if row_stop is None:
                if timestamp is None:
                    scan_id = client.scannerOpen(self.name, row_start, columns)
                else:
                    scan_id = client.scannerOpenTs(
                        self.name, row_start, columns, timestamp)
            else:
                if timestamp is None:
                    scan_id = client.scannerOpenWithStop(
                        self.name, row_start, row_stop, columns)
                else:
                    scan_id = client.scannerOpenWithStopTs(
                        self.name, row_start, row_stop, columns, timestamp)

        else:
            # The scan's caching size is set to the batch_size, so that
            # the HTable on the Java side retrieves rows from the region
            # servers in the same chunk sizes that it sends out over
            # Thrift.
            scan = TScan(startRow=row_start,
                         stopRow=row_stop,
                         timestamp=timestamp,
                         columns=columns,
                         caching=batch_size,
                         filterString=filter)
            scan_id = client.scannerOpenWithScan(self.name, scan)

        logger.debug("Opened scanner (id=%d) on '%s'", scan_id, self.name)

        n_results = 0
        try:
            while True:
                if limit is None:
                    how_many = batch_size
                else:
                    how_many = min(batch_size, limit - n_results)

                if how_many == 1:
                    items = client.scannerGet(scan_id)
                else:
                    items = client.scannerGetList(scan_id, how_many)

                for item in items:
                    n_results += 1
                    yield item.row, _make_row(item.columns, include_timestamp)
                    if limit is not None and n_results == limit:
                        return

                # Avoid round-trip when exhausted
                if len(items) < how_many:
                    break
        finally:
            client.scannerClose(scan_id)
            logger.debug("Closed scanner (id=%d) on '%s'", scan_id, self.name)

    #
    # Data manipulation
    #

    def put(self, row, data, timestamp=None):
        """Stores data in the table.

        This method stores the data in the `data` argument for the row
        specified by `row`. The `data` argument is dictionary that maps columns
        to values. Column names must include a family and qualifier part, e.g.
        `cf:col`, though the qualifier part may be the empty string, e.g.
        `cf:`. The `timestamp` argument is optional.

        Note that, in many situations, :py:meth:`batch()` is a more appropriate
        method to manipulate data.

        :param str row: the row key
        :param dict data: the data to store
        :param int timestamp: timestamp (optional)
        """
        with self.batch(timestamp=timestamp) as batch:
            batch.put(row, data)

    def delete(self, row, columns=None, timestamp=None):
        """Deletes data from the table.

        This method deletes all columns for the row specified by `row`, or only
        some columns if the `columns` argument is specified.

        Note that, in many situations, :py:meth:`batch()` is a more appropriate
        method to manipulate data.

        :param str row: the row key
        :param list_or_tuple columns: list of columns (optional)
        :param int timestamp: timestamp (optional)
        """
        if columns is None:
            if timestamp is None:
                self.client.deleteAllRow(self.name, row)
            else:
                self.client.deleteAllRowTs(self.name, row, timestamp)
        else:
            with self.batch(timestamp=timestamp) as batch:
                batch.delete(row, columns)

    def batch(self, timestamp=None, batch_size=None, transaction=False):
        """Creates a new batch instance for this table.

        This method returns a new :py:class:`Batch` instance that can be used
        for mass data manipulation. The `timestamp` argument applies to all
        puts and deletes on the batch.

        If given, the `batch_size` argument specifies the maximum batch size
        after which the batch should send the mutations to the server. By
        default this is unbounded.

        The `transaction` argument specifies whether the returned
        :py:class:`Batch` instance should act in a transaction-like manner when
        used as context manager in a ``with`` block of code. The `transaction`
        flag cannot be used in combination with `batch_size`.

        :param bool transaction: whether this batch should behave like
                                 a transaction (only useful when used as a
                                 context manager)
        :param int timestamp: timestamp (optional)

        :return: Batch instance
        :rtype: :py:class:`Batch`
        """
        return Batch(self, timestamp, batch_size, transaction)

    #
    # Atomic counters
    #

    def counter_get(self, row, column):
        """Retrieves the current value of a counter column.

        This method retrieves the current value of a counter column. If the
        counter column does not exist, this function initialises it to `0`.

        Note that application code should *never* store a incremented or
        decremented counter value directly; use the atomic
        :py:meth:`Table.counter_inc` and :py:meth:`Table.counter_dec` methods
        for that.

        :param str row: the row key
        :param str column: the column name

        :return: counter value
        :rtype: int
        """
        # Don't query directly, but increment with value=0 so that the counter
        # is correctly initialised if didn't exist yet.
        return self.counter_inc(row, column, value=0)

    def counter_set(self, row, column, value=0):
        """Sets a counter column to a specific value.

        This method stores a 64-bit signed integer value in the specified
        column.

        Note that application code should *never* store a incremented or
        decremented counter value directly; use the atomic
        :py:meth:`Table.counter_inc` and :py:meth:`Table.counter_dec` methods
        for that.

        :param str row: the row key
        :param str column: the column name
        :param int value: the counter value to set
        """
        self.put(row, {column: pack_i64(value)})

    def counter_inc(self, row, column, value=1):
        """Atomically increments (or decrements) a counter column.

        This method atomically increments or decrements a counter column in the
        row specified by `row`. The `value` argument specifies how much the
        counter should be incremented (for positive values) or decremented (for
        negative values). If the counter column did not exist, it is
        automatically initialised to 0 before incrementing it.

        :param str row: the row key
        :param str column: the column name
        :param int value: the amount to increment or decrement by (optional)

        :return: counter value after incrementing
        :rtype: int
        """
        return self.client.atomicIncrement(self.name, row, column, value)

    def counter_dec(self, row, column, value=1):
        """Atomically decrements (or increments) a counter column.

        This method is a shortcut for calling :py:meth:`Table.counter_inc` with
        the value negated.

        :return: counter value after decrementing
        :rtype: int
        """
        return self.counter_inc(row, column, -value)


class Batch:
    """Batch mutation class.

    This class cannot be instantiated directly; use :py:meth:`Table.batch`
    instead.
    """
    def __init__(self, table, timestamp=None, batch_size=None,
                 transaction=False):
        """Initialises a new Batch instance."""
        if not (timestamp is None or isinstance(timestamp, int)):
            raise TypeError("'timestamp' must be an integer or None")

        if batch_size is not None:
            if transaction:
                raise TypeError("'transaction' can only be used when no 'batch_size' is specified")
            if not batch_size > 0:
                raise ValueError("'batch_size' must be >= 1")

        self.table = table
        self.batch_size = batch_size
        self.timestamp = timestamp
        self.transaction = transaction
        self._families = None
        self._reset_mutations()

    def _reset_mutations(self):
        """Resets the internal mutation buffer."""
        self._mutations = defaultdict(list)
        self._mutation_count = 0

    def send(self):
        """Sends the batch to the server."""
        bms = [BatchMutation(row, m) for row, m in self._mutations.iteritems()]
        if not bms:
            return

        logger.debug("Sending batch for '%s' (%d mutations on %d rows)",
                     self.table.name, self._mutation_count, len(bms))
        if self.timestamp is None:
            self.table.client.mutateRows(self.table.name, bms)
        else:
            self.table.client.mutateRowsTs(self.table.name, bms,
                                           self.timestamp)

        self._reset_mutations()

    #
    # Mutation methods
    #

    def put(self, row, data):
        """Stores data in the table.

        See :py:meth:`Table.put` for a description of the `row` and `data`
        arguments.
        """
        self._mutations[row].extend(
            Mutation(isDelete=False, column=column, value=value)
            for column, value in data.iteritems())

        self._mutation_count += len(data)
        if self.batch_size and self._mutation_count >= self.batch_size:
            self.send()

    def delete(self, row, columns=None):
        """Deletes data from the table.

        See :py:meth:`Table.delete` for a description of the `row` and `data`
        arguments.
        """
        # Work-around Thrift API limitation: the mutation API can only
        # delete specified columns, not complete rows, so just list the
        # column families once and cache them for later use in the same
        # transaction.
        if columns is None:
            if self._families is None:
                self._families = self.table._column_family_names()
            columns = self._families

        self._mutations[row].extend(
            Mutation(isDelete=True, column=column) for column in columns)

        self._mutation_count += len(columns)
        if self.batch_size and self._mutation_count >= self.batch_size:
            self.send()

    #
    # Context manager methods
    #

    def __enter__(self):
        """Called upon entering a ``with`` block"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Called upon exiting a ``with`` block"""
        # If the 'with' block raises an exception, the batch will not be
        # sent to the server.
        if self.transaction and exc_type is not None:
            return

        self.send()
