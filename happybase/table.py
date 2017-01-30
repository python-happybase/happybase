"""
HappyBase table module.
"""

import logging
from numbers import Integral
from struct import Struct

from six import iteritems

from Hbase_thrift import TScan

from .util import thrift_type_to_dict, bytes_increment, OrderedDict
from .batch import Batch

logger = logging.getLogger(__name__)

pack_i64 = Struct('>q').pack


def make_row(cell_map, include_timestamp):
    """Make a row dict for a cell mapping like ttypes.TRowResult.columns."""
    return {
        name: (cell.value, cell.timestamp) if include_timestamp else cell.value
        for name, cell in iteritems(cell_map)
    }


def make_ordered_row(sorted_columns, include_timestamp):
    """Make a row dict for sorted column results from scans."""
    od = OrderedDict()
    for column in sorted_columns:
        if include_timestamp:
            value = (column.cell.value, column.cell.timestamp)
        else:
            value = column.cell.value
        od[column.columnName] = value
    return od


class Table(object):
    """HBase table abstraction class.

    This class cannot be instantiated directly; use :py:meth:`Connection.table`
    instead.
    """
    def __init__(self, name, connection):
        self.name = name
        self.connection = connection

    def __repr__(self):
        return '<%s.%s name=%r>' % (
            __name__,
            self.__class__.__name__,
            self.name,
        )

    def families(self):
        """Retrieve the column families for this table.

        :return: Mapping from column family name to settings dict
        :rtype: dict
        """
        descriptors = self.connection.client.getColumnDescriptors(self.name)
        families = dict()
        for name, descriptor in descriptors.items():
            name = name.rstrip(b':')
            families[name] = thrift_type_to_dict(descriptor)
        return families

    def _column_family_names(self):
        """Retrieve the column family names for this table (internal use)"""
        names = self.connection.client.getColumnDescriptors(self.name).keys()
        return [name.rstrip(b':') for name in names]

    def regions(self):
        """Retrieve the regions for this table.

        :return: regions for this table
        :rtype: list of dicts
        """
        regions = self.connection.client.getTableRegions(self.name)
        return [thrift_type_to_dict(r) for r in regions]

    #
    # Data retrieval
    #

    def row(self, row, columns=None, timestamp=None, include_timestamp=False):
        """Retrieve a single row of data.

        This method retrieves the row with the row key specified in the `row`
        argument and returns the columns and values for this row as
        a dictionary.

        The `row` argument is the row key of the row. If the `columns`
        argument is specified, only the values for these columns will be
        returned instead of all available columns. The `columns`
        argument should be a list or tuple containing byte strings. Each
        name can be a column family, such as ``b'cf1'`` or ``b'cf1:'``
        (the trailing colon is not required), or a column family with a
        qualifier, such as ``b'cf1:col1'``.

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
            rows = self.connection.client.getRowWithColumns(
                self.name, row, columns, {})
        else:
            if not isinstance(timestamp, Integral):
                raise TypeError("'timestamp' must be an integer")
            rows = self.connection.client.getRowWithColumnsTs(
                self.name, row, columns, timestamp, {})

        if not rows:
            return {}

        return make_row(rows[0].columns, include_timestamp)

    def rows(self, rows, columns=None, timestamp=None,
             include_timestamp=False):
        """Retrieve multiple rows of data.

        This method retrieves the rows with the row keys specified in the
        `rows` argument, which should be should be a list (or tuple) of row
        keys. The return value is a list of `(row_key, row_dict)` tuples.

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
            results = self.connection.client.getRowsWithColumns(
                self.name, rows, columns, {})
        else:
            if not isinstance(timestamp, Integral):
                raise TypeError("'timestamp' must be an integer")

            # Work-around a bug in the HBase Thrift server where the
            # timestamp is only applied if columns are specified, at
            # the cost of an extra round-trip.
            if columns is None:
                columns = self._column_family_names()

            results = self.connection.client.getRowsWithColumnsTs(
                self.name, rows, columns, timestamp, {})

        return [(r.row, make_row(r.columns, include_timestamp))
                for r in results]

    def cells(self, row, column, versions=None, timestamp=None,
              include_timestamp=False):
        """Retrieve multiple versions of a single cell from the table.

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
            raise TypeError("'versions' argument must be a number or None")
        elif versions < 1:
            raise ValueError(
                "'versions' argument must be at least 1 (or None)")

        if timestamp is None:
            cells = self.connection.client.getVer(
                self.name, row, column, versions, {})
        else:
            if not isinstance(timestamp, Integral):
                raise TypeError("'timestamp' must be an integer")
            cells = self.connection.client.getVerTs(
                self.name, row, column, timestamp, versions, {})

        return [
            (c.value, c.timestamp) if include_timestamp else c.value
            for c in cells
        ]

    def scan(self, row_start=None, row_stop=None, row_prefix=None,
             columns=None, filter=None, timestamp=None,
             include_timestamp=False, batch_size=1000, scan_batching=None,
             limit=None, sorted_columns=False, reverse=False):
        """Create a scanner for data in the table.

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

        The `batch_size` argument specifies how many results should be
        retrieved per batch when retrieving results from the scanner. Only set
        this to a low value (or even 1) if your data is large, since a low
        batch size results in added round-trips to the server.

        The optional `scan_batching` is for advanced usage only; it
        translates to `Scan.setBatching()` at the Java side (inside the
        Thrift server). By setting this value rows may be split into
        partial rows, so result rows may be incomplete, and the number
        of results returned by te scanner may no longer correspond to
        the number of rows matched by the scan.

        If `sorted_columns` is `True`, the columns in the rows returned
        by this scanner will be retrieved in sorted order, and the data
        will be stored in `OrderedDict` instances.

        If `reverse` is `True`, the scanner will perform the scan in reverse.
        This means that `row_start` must be lexicographically after `row_stop`.
        Note that the start of the range is inclusive, while the end is
        exclusive just as in the forward scan.

        **Compatibility notes:**

        * The `filter` argument is only available when using HBase 0.92
          (or up). In HBase 0.90 compatibility mode, specifying
          a `filter` raises an exception.

        * The `sorted_columns` argument is only available when using
          HBase 0.96 (or up).

        * The `reverse` argument is only available when using HBase 0.98
          (or up).

        .. versionadded:: TODO
           `reverse` argument

        .. versionadded:: 0.8
           `sorted_columns` argument

        .. versionadded:: 0.8
           `scan_batching` argument

        :param str row_start: the row key to start at (inclusive)
        :param str row_stop: the row key to stop at (exclusive)
        :param str row_prefix: a prefix of the row key that must match
        :param list_or_tuple columns: list of columns (optional)
        :param str filter: a filter string (optional)
        :param int timestamp: timestamp (optional)
        :param bool include_timestamp: whether timestamps are returned
        :param int batch_size: batch size for retrieving results
        :param bool scan_batching: server-side scan batching (optional)
        :param int limit: max number of rows to return
        :param bool sorted_columns: whether to return sorted columns
        :param bool reverse: whether to perform scan in reverse

        :return: generator yielding the rows matching the scan
        :rtype: iterable of `(row_key, row_data)` tuples
        """
        if batch_size < 1:
            raise ValueError("'batch_size' must be >= 1")

        if limit is not None and limit < 1:
            raise ValueError("'limit' must be >= 1")

        if scan_batching is not None and scan_batching < 1:
            raise ValueError("'scan_batching' must be >= 1")

        if sorted_columns and self.connection.compat < '0.96':
            raise NotImplementedError(
                "'sorted_columns' is only supported in HBase >= 0.96")

        if reverse and self.connection.compat < '0.98':
            raise NotImplementedError(
                "'reverse' is only supported in HBase >= 0.98")

        if row_prefix is not None:
            if row_start is not None or row_stop is not None:
                raise TypeError(
                    "'row_prefix' cannot be combined with 'row_start' "
                    "or 'row_stop'")

            if reverse:
                row_start = bytes_increment(row_prefix)
                row_stop = row_prefix
            else:
                row_start = row_prefix
                row_stop = bytes_increment(row_prefix)

        if row_start is None:
            row_start = ''

        if self.connection.compat == '0.90':
            # The scannerOpenWithScan() Thrift function is not
            # available, so work around it as much as possible with the
            # other scannerOpen*() Thrift functions

            if filter is not None:
                raise NotImplementedError(
                    "'filter' is not supported in HBase 0.90")

            if row_stop is None:
                if timestamp is None:
                    scan_id = self.connection.client.scannerOpen(
                        self.name, row_start, columns, {})
                else:
                    scan_id = self.connection.client.scannerOpenTs(
                        self.name, row_start, columns, timestamp, {})
            else:
                if timestamp is None:
                    scan_id = self.connection.client.scannerOpenWithStop(
                        self.name, row_start, row_stop, columns, {})
                else:
                    scan_id = self.connection.client.scannerOpenWithStopTs(
                        self.name, row_start, row_stop, columns, timestamp, {})

        else:
            # XXX: The "batch_size" can be slightly confusing to those
            # familiar with the HBase Java API:
            #
            # * TScan.caching (Thrift API) translates to
            #   Scan.setCaching() (Java API)
            #
            # * TScan.batchSize (Thrift API) translates to
            #   Scan.setBatching (Java API) .
            #
            # However, we set Scan.setCaching() to what is called
            # batch_size in the HappyBase API, so that the HTable on the
            # Java side (inside the Thrift server) retrieves rows from
            # the region servers in the same chunk sizes that it sends
            # out again to Python (over Thrift). This cannot be tweaked
            # (by design).
            #
            # The Scan.setBatching() value (Java API), which possibly
            # cuts rows into multiple partial rows, can be set using the
            # slightly strange name scan_batching.
            scan = TScan(
                startRow=row_start,
                stopRow=row_stop,
                timestamp=timestamp,
                columns=columns,
                caching=batch_size,
                filterString=filter,
                batchSize=scan_batching,
                sortColumns=sorted_columns,
                reversed=reverse,
            )
            scan_id = self.connection.client.scannerOpenWithScan(
                self.name, scan, {})

        logger.debug("Opened scanner (id=%d) on '%s'", scan_id, self.name)

        n_returned = n_fetched = 0
        try:
            while True:
                if limit is None:
                    how_many = batch_size
                else:
                    how_many = min(batch_size, limit - n_returned)

                items = self.connection.client.scannerGetList(
                    scan_id, how_many)

                if not items:
                    return  # scan has finished

                n_fetched += len(items)

                for n_returned, item in enumerate(items, n_returned + 1):
                    if sorted_columns:
                        row = make_ordered_row(item.sortedColumns,
                                               include_timestamp)
                    else:
                        row = make_row(item.columns, include_timestamp)

                    yield item.row, row

                    if limit is not None and n_returned == limit:
                        return  # scan has finished
        finally:
            self.connection.client.scannerClose(scan_id)
            logger.debug(
                "Closed scanner (id=%d) on '%s' (%d returned, %d fetched)",
                scan_id, self.name, n_returned, n_fetched)

    #
    # Data manipulation
    #

    def put(self, row, data, timestamp=None, wal=True):
        """Store data in the table.

        This method stores the data in the `data` argument for the row
        specified by `row`. The `data` argument is dictionary that maps columns
        to values. Column names must include a family and qualifier part, e.g.
        ``b'cf:col'``, though the qualifier part may be the empty string, e.g.
        ``b'cf:'``.

        Note that, in many situations, :py:meth:`batch()` is a more appropriate
        method to manipulate data.

        .. versionadded:: 0.7
           `wal` argument

        :param str row: the row key
        :param dict data: the data to store
        :param int timestamp: timestamp (optional)
        :param wal bool: whether to write to the WAL (optional)
        """
        with self.batch(timestamp=timestamp, wal=wal) as batch:
            batch.put(row, data)

    def delete(self, row, columns=None, timestamp=None, wal=True):
        """Delete data from the table.

        This method deletes all columns for the row specified by `row`, or only
        some columns if the `columns` argument is specified.

        Note that, in many situations, :py:meth:`batch()` is a more appropriate
        method to manipulate data.

        .. versionadded:: 0.7
           `wal` argument

        :param str row: the row key
        :param list_or_tuple columns: list of columns (optional)
        :param int timestamp: timestamp (optional)
        :param wal bool: whether to write to the WAL (optional)
        """
        with self.batch(timestamp=timestamp, wal=wal) as batch:
            batch.delete(row, columns)

    def batch(self, timestamp=None, batch_size=None, transaction=False,
              wal=True):
        """Create a new batch operation for this table.

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

        The `wal` argument determines whether mutations should be
        written to the HBase Write Ahead Log (WAL). This flag can only
        be used with recent HBase versions. If specified, it provides
        a default for all the put and delete operations on this batch.
        This default value can be overridden for individual operations
        using the `wal` argument to :py:meth:`Batch.put` and
        :py:meth:`Batch.delete`.

        .. versionadded:: 0.7
           `wal` argument

        :param bool transaction: whether this batch should behave like
                                 a transaction (only useful when used as a
                                 context manager)
        :param int batch_size: batch size (optional)
        :param int timestamp: timestamp (optional)
        :param wal bool: whether to write to the WAL (optional)

        :return: Batch instance
        :rtype: :py:class:`Batch`
        """
        kwargs = locals().copy()
        del kwargs['self']
        return Batch(table=self, **kwargs)

    #
    # Atomic counters
    #

    def counter_get(self, row, column):
        """Retrieve the current value of a counter column.

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
        """Set a counter column to a specific value.

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
        """Atomically increment (or decrements) a counter column.

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
        return self.connection.client.atomicIncrement(
            self.name, row, column, value)

    def counter_dec(self, row, column, value=1):
        """Atomically decrement (or increments) a counter column.

        This method is a shortcut for calling :py:meth:`Table.counter_inc` with
        the value negated.

        :return: counter value after decrementing
        :rtype: int
        """
        return self.counter_inc(row, column, -value)
