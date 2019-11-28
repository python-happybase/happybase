"""
AIOHappyBase Batch module.
"""

import logging
from typing import TYPE_CHECKING, Dict, List, Iterable, Type
from functools import partial
from collections import defaultdict
from numbers import Integral

from Hbase_thrift import BatchMutation, Mutation

if TYPE_CHECKING:
    from .table import Table  # Avoid circular import

logger = logging.getLogger(__name__)


class Batch:
    """
    Batch mutation class.

    This class cannot be instantiated directly;
    use :py:meth:`Table.batch` instead.
    """
    def __init__(self,
                 table: 'Table',
                 timestamp: int = None,
                 batch_size: int = None,
                 transaction: bool = False,
                 wal: bool = True):
        """Initialise a new Batch instance."""
        if not (timestamp is None or isinstance(timestamp, Integral)):
            raise TypeError("'timestamp' must be an integer or None")

        if batch_size is not None:
            if transaction:
                raise TypeError("'transaction' can't be used with 'batch_size'")
            if not batch_size > 0:
                raise ValueError("'batch_size' must be > 0")

        self._table = table
        self._batch_size = batch_size
        self._timestamp = timestamp
        self._transaction = transaction
        self._wal = wal
        self._families = None
        self._reset_mutations()

        # Save mutator partial here to avoid the if check each time
        if self._timestamp is None:
            self._mutate_rows = partial(
                self._table.client.mutateRows,
                self._table.name,
                attributes={},
            )
        else:
            self._mutate_rows = partial(
                self._table.client.mutateRowsTs,
                self._table.name,
                timestamp=self._timestamp,
                attributes={},
            )

    def _reset_mutations(self) -> None:
        """Reset the internal mutation buffer."""
        self._mutations = defaultdict(list)
        self._mutation_count = 0

    async def send(self) -> None:
        """Send the batch to the server."""
        bms = [BatchMutation(row, m) for row, m in self._mutations.items()]
        if not bms:
            return

        logger.debug(
            f"Sending batch for '{self._table.name}' ({self._mutation_count} "
            f"mutations on {len(bms)} rows)"
        )

        await self._mutate_rows(bms)
        self._reset_mutations()

    async def put(self,
                  row: bytes,
                  data: Dict[bytes, bytes],
                  wal: bool = None) -> None:
        """
        Store data in the table.

        See :py:meth:`Table.put` for a description of the `row`, `data`,
        and `wal` arguments. The `wal` argument should normally not be
        used; its only use is to override the batch-wide value passed to
        :py:meth:`Table.batch`.
        """
        if wal is None:
            wal = self._wal

        await self._add_mutations(row, [
            Mutation(isDelete=False, column=column, value=value, writeToWAL=wal)
            for column, value in data.items()
        ])

    async def delete(self,
                     row: bytes,
                     columns: Iterable[bytes] = None,
                     wal: bool = None) -> None:
        """
        Delete data from the table.

        See :py:meth:`Table.put` for a description of the `row`, `data`,
        and `wal` arguments. The `wal` argument should normally not be
        used; its only use is to override the batch-wide value passed to
        :py:meth:`Table.batch`.
        """
        # Work-around Thrift API limitation: the mutation API can only
        # delete specified columns, not complete rows, so just list the
        # column families once and cache them for later use by the same
        # batch instance.
        if columns is None:
            if self._families is None:
                self._families = await self._table._column_family_names()
            columns = self._families

        if wal is None:
            wal = self._wal

        await self._add_mutations(row, [
            Mutation(isDelete=True, column=column, writeToWAL=wal)
            for column in columns
        ])

    async def _add_mutations(self, row: bytes, mutations: List[Mutation]):
        self._mutations[row].extend(mutations)
        self._mutation_count += len(mutations)
        if self._batch_size and self._mutation_count >= self._batch_size:
            await self.send()

    async def close(self) -> None:
        """Finalize the batch and make sure all tasks are completed."""
        await self.send()  # Send any remaining mutations

    # Support usage as an async context manager
    async def __aenter__(self) -> 'Batch':
        """Called upon entering a ``async with`` block"""
        return self

    async def __aexit__(self, exc_type: Type[Exception], *_) -> None:
        """Called upon exiting a ``async with`` block"""
        # If the 'with' block raises an exception, the batch will not be
        # sent to the server.
        if self._transaction and exc_type is not None:
            return

        await self.close()

    # Guard against porting mistakes
    def __enter__(self):
        raise RuntimeError("Use async with")

    def __exit__(self, *_exc):
        raise RuntimeError("Use async with")
