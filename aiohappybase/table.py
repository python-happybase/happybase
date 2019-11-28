"""
AIOHappyBase table module.
"""

import logging
from numbers import Integral
from struct import Struct
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Tuple,
    Any,
    Iterable,
    Union,
    AsyncGenerator as AsyncGen,
)

from Hbase_thrift import TScan, TRowResult

from ._util import thrift_type_to_dict, bytes_increment, map_dict
from .batch import Batch

if TYPE_CHECKING:
    from .connection import Connection

logger = logging.getLogger(__name__)

Data = Dict[bytes, bytes]
Row = Union[Dict[bytes, bytes], Dict[bytes, Tuple[bytes, int]]]

pack_i64 = Struct('>q').pack


def make_row(row: TRowResult, include_timestamp: bool) -> Row:
    """Make a row dict for a given row result."""
    if row.sortedColumns is not None:
        cell_map = {c.columnName: c.cell for c in row.sortedColumns}
    elif row.columns is not None:
        cell_map = row.columns
    else:
        raise RuntimeError("Neither columns nor sortedColumns is available!")
    return {
        name: (cell.value, cell.timestamp) if include_timestamp else cell.value
        for name, cell in cell_map.items()
    }


class Table:
    """
    HBase table abstraction class.

    This class cannot be instantiated directly;
    use :py:meth:`Connection.table` instead.
    """
    def __init__(self, name: bytes, connection: 'Connection'):
        self.name = name
        self.connection = connection

    @property
    def client(self):
        return self.connection.client

    def __repr__(self):
        return f'<{__name__}.{self.__class__.__name__} name={self.name!r}>'

    async def families(self) -> Dict[bytes, Dict[bytes, Any]]:
        """
        Retrieve the column families for this table.

        :return: Mapping from column family name to settings dict
        """
        descriptors = await self._column_family_descriptors()
        return map_dict(descriptors, values=thrift_type_to_dict)

    async def _column_family_names(self) -> List[bytes]:
        """Retrieve the column family names for this table"""
        return list(await self._column_family_descriptors())

    async def _column_family_descriptors(self) -> Dict[bytes, Any]:
        """Retrieve the column family descriptors for this table"""
        descriptors = await self.client.getColumnDescriptors(self.name)
        return map_dict(descriptors, keys=lambda k: k.rstrip(b':'))

    async def regions(self) -> List[Dict[bytes, Any]]:
        """
        Retrieve the regions for this table.

        :return: regions for this table
        """
        regions = await self.client.getTableRegions(self.name)
        return [thrift_type_to_dict(r) for r in regions]

    # Data retrieval methods

    async def row(self,
                  row: bytes,
                  columns: Iterable[bytes] = None,
                  timestamp: int = None,
                  include_timestamp: bool = False) -> Row:
        """
        Retrieve a single row of data.

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

        :param row: the row key
        :param columns: list of columns (optional)
        :param timestamp: timestamp (optional)
        :param include_timestamp: whether timestamps are returned

        :return: Mapping of columns (both qualifier and family) to values
        """
        if columns is not None and not isinstance(columns, (tuple, list)):
            raise TypeError("'columns' must be a tuple or list")

        if timestamp is None:
            rows = await self.client.getRowWithColumns(
                self.name, row, columns, {})
        else:
            if not isinstance(timestamp, Integral):
                raise TypeError("'timestamp' must be an integer")
            rows = await self.client.getRowWithColumnsTs(
                self.name, row, columns, timestamp, {})

        if not rows:
            return {}

        return make_row(rows[0], include_timestamp)

    async def rows(self,
                   rows: List[bytes],
                   columns: Iterable[bytes] = None,
                   timestamp: int = None,
                   include_timestamp: bool = False) -> List[Tuple[bytes, Row]]:
        """
        Retrieve multiple rows of data.

        This method retrieves the rows with the row keys specified in the
        `rows` argument, which should be should be a list (or tuple) of row
        keys. The return value is a list of `(row_key, row_dict)` tuples.

        The `columns`, `timestamp` and `include_timestamp` arguments behave
        exactly the same as for :py:meth:`row`.

        :param rows: list of row keys
        :param columns: list of columns (optional)
        :param timestamp: timestamp (optional)
        :param include_timestamp: whether timestamps are returned

        :return: List of mappings (columns to values)
        """
        if columns is not None and not isinstance(columns, (tuple, list)):
            raise TypeError("'columns' must be a tuple or list")

        if not rows:
            # Avoid round-trip if the result is empty anyway
            return []

        if timestamp is None:
            results = await self.client.getRowsWithColumns(
                self.name, rows, columns, {})
        else:
            if not isinstance(timestamp, Integral):
                raise TypeError("'timestamp' must be an integer")

            # Work-around a bug in the HBase Thrift server where the
            # timestamp is only applied if columns are specified, at
            # the cost of an extra round-trip.
            if columns is None:
                columns = await self._column_family_names()

            results = await self.client.getRowsWithColumnsTs(
                self.name, rows, columns, timestamp, {})

        return [(r.row, make_row(r, include_timestamp)) for r in results]

    async def cells(self,
                    row: bytes,
                    column: bytes,
                    versions: int = None,
                    timestamp: int = None,
                    include_timestamp: bool = False) -> List[Tuple[bytes, int]]:
        """
        Retrieve multiple versions of a single cell from the table.

        This method retrieves multiple versions of a cell (if any).

        The `versions` argument defines how many cell versions to
        retrieve at most.

        The `timestamp` and `include_timestamp` arguments behave exactly the
        same as for :py:meth:`row`.

        :param row: the row key
        :param column: the column name
        :param versions: the maximum number of versions to retrieve
        :param timestamp: timestamp (optional)
        :param include_timestamp: whether timestamps are returned

        :return: cell values
        """
        if versions is None:
            versions = (2 ** 31) - 1  # Thrift type is i32
        elif not isinstance(versions, int):
            raise TypeError("'versions' argument must be a number or None")
        elif versions < 1:
            raise ValueError(
                "'versions' argument must be at least 1 (or None)")

        if timestamp is None:
            cells = await self.client.getVer(
                self.name, row, column, versions, {})
        else:
            if not isinstance(timestamp, Integral):
                raise TypeError("'timestamp' must be an integer")
            cells = await self.client.getVerTs(
                self.name, row, column, timestamp, versions, {})

        return [
            (c.value, c.timestamp) if include_timestamp else c.value
            for c in cells
        ]

    async def scan(self,
                   row_start: bytes = None,
                   row_stop: bytes = None,
                   row_prefix: bytes = None,
                   columns: Iterable[bytes] = None,
                   filter: bytes = None,  # noqa
                   timestamp: int = None,
                   include_timestamp: bool = False,
                   batch_size: int = 1000,
                   scan_batching: int = None,
                   limit: int = None,
                   sorted_columns: bool = False,
                   reverse: bool = False) -> AsyncGen[Tuple[bytes, Data], None]:
        """
        Create a scanner for data in the table.

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
        of results returned by the scanner may no longer correspond to
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

        .. versionadded:: 1.1.0
           `reverse` argument

        .. versionadded:: 0.8
           `sorted_columns` argument

        .. versionadded:: 0.8
           `scan_batching` argument

        :param row_start: the row key to start at (inclusive)
        :param row_stop: the row key to stop at (exclusive)
        :param row_prefix: a prefix of the row key that must match
        :param columns: list of columns (optional)
        :param filter: a filter string (optional)
        :param timestamp: timestamp (optional)
        :param include_timestamp: whether timestamps are returned
        :param batch_size: batch size for retrieving results
        :param scan_batching: server-side scan batching (optional)
        :param limit: max number of rows to return
        :param sorted_columns: whether to return sorted columns
        :param reverse: whether to perform scan in reverse

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
            raise NotImplementedError("'sorted_columns' requires HBase >= 0.96")

        if reverse and self.connection.compat < '0.98':
            raise NotImplementedError("'reverse' requires HBase >= 0.98")

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
            row_start = b''

        if self.connection.compat == '0.90':
            # The scannerOpenWithScan() Thrift function is not
            # available, so work around it as much as possible with the
            # other scannerOpen*() Thrift functions

            if filter is not None:
                raise NotImplementedError("'filter' requires HBase > 0.90")

            args = [self.name, row_start, columns]
            scan_func = 'scannerOpen'

            if row_stop is not None:
                scan_func += 'WithStop'
                args.insert(2, row_stop)

            if timestamp is not None:
                scan_func += 'Ts'
                args.append(timestamp)

            scan_id = await getattr(self.client, scan_func)(*args, {})

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
            scan_id = await self.client.scannerOpenWithScan(self.name, scan, {})

        logger.debug(f"Opened scanner (id={scan_id}) on '{self.name}'")

        n_returned = n_fetched = 0
        try:
            while True:
                if limit is None:
                    how_many = batch_size
                else:
                    how_many = min(batch_size, limit - n_returned)

                items = await self.client.scannerGetList(scan_id, how_many)

                if not items:
                    return  # scan has finished

                n_fetched += len(items)

                for n_returned, item in enumerate(items, n_returned + 1):
                    yield item.row, make_row(item, include_timestamp)

                    if limit is not None and n_returned == limit:
                        return  # scan has finished
        finally:
            await self.client.scannerClose(scan_id)
            logger.debug(
                f"Closed scanner (id={scan_id}) on '{self.name}' "
                f"({n_returned} returned, {n_fetched} fetched)"
            )

    # Data manipulation methods

    async def put(self,
                  row: bytes,
                  data: Data,
                  timestamp: int = None,
                  wal: bool = True) -> None:
        """
        Store data in the table.

        This method stores the data in the `data` argument for the row
        specified by `row`. The `data` argument is dictionary that maps columns
        to values. Column names must include a family and qualifier part, e.g.
        ``b'cf:col'``, though the qualifier part may be the empty string, e.g.
        ``b'cf:'``.

        Note that, in many situations, :py:meth:`batch()` is a more appropriate
        method to manipulate data.

        .. versionadded:: 0.7
           `wal` argument

        :param row: the row key
        :param data: the data to store
        :param timestamp: timestamp (optional)
        :param wal: whether to write to the WAL (optional)
        """
        async with self.batch(timestamp=timestamp, wal=wal) as batch:
            await batch.put(row, data)

    async def delete(self,
                     row: bytes,
                     columns: Iterable[bytes] = None,
                     timestamp: int = None,
                     wal: bool = True) -> None:
        """
        Delete data from the table.

        This method deletes all columns for the row specified by `row`, or only
        some columns if the `columns` argument is specified.

        Note that, in many situations, :py:meth:`batch()` is a more appropriate
        method to manipulate data.

        .. versionadded:: 0.7
           `wal` argument

        :param row: the row key
        :param columns: list of columns (optional)
        :param timestamp: timestamp (optional)
        :param wal: whether to write to the WAL (optional)
        """
        async with self.batch(timestamp=timestamp, wal=wal) as batch:
            await batch.delete(row, columns)

    def batch(self,
              timestamp: int = None,
              batch_size: int = None,
              transaction: bool = False,
              wal: bool = True) -> Batch:
        """
        Create a new batch operation for this table.

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

        :param transaction:
            whether this batch should behave like a transaction
            (only useful when used as a context manager)
        :param batch_size: batch size (optional)
        :param timestamp: timestamp (optional)
        :param wal: whether to write to the WAL (optional)

        :return: Batch instance
        """
        kwargs = locals().copy()
        del kwargs['self']
        return Batch(table=self, **kwargs)

    # Atomic counters

    async def counter_get(self, row: bytes, column: bytes) -> int:
        """
        Retrieve the current value of a counter column.

        This method retrieves the current value of a counter column. If the
        counter column does not exist, this function initialises it to `0`.

        Note that application code should *never* store a incremented or
        decremented counter value directly; use the atomic
        :py:meth:`Table.counter_inc` and :py:meth:`Table.counter_dec` methods
        for that.

        :param row: the row key
        :param column: the column name

        :return: counter value
        """
        # Don't query directly, but increment with value=0 so that the counter
        # is correctly initialised if didn't exist yet.
        return await self.counter_inc(row, column, value=0)

    async def counter_set(self,
                          row: bytes,
                          column: bytes,
                          value: int = 0) -> None:
        """
        Set a counter column to a specific value.

        This method stores a 64-bit signed integer value in the specified
        column.

        Note that application code should *never* store a incremented or
        decremented counter value directly; use the atomic
        :py:meth:`Table.counter_inc` and :py:meth:`Table.counter_dec` methods
        for that.

        :param row: the row key
        :param column: the column name
        :param value: the counter value to set
        """
        await self.put(row, {column: pack_i64(value)})

    async def counter_inc(self,
                          row: bytes,
                          column: bytes,
                          value: int = 1) -> int:
        """
        Atomically increment (or decrements) a counter column.

        This method atomically increments or decrements a counter column in the
        row specified by `row`. The `value` argument specifies how much the
        counter should be incremented (for positive values) or decremented (for
        negative values). If the counter column did not exist, it is
        automatically initialised to 0 before incrementing it.

        :param row: the row key
        :param column: the column name
        :param value: the amount to increment or decrement by (optional)

        :return: counter value after incrementing
        """
        return await self.client.atomicIncrement(self.name, row, column, value)

    async def counter_dec(self,
                          row: bytes,
                          column: bytes,
                          value: int = 1) -> int:
        """
        Atomically decrement (or increments) a counter column.

        This method is a shortcut for calling :py:meth:`Table.counter_inc` with
        the value negated.

        :return: counter value after decrementing
        """
        return await self.counter_inc(row, column, -value)
