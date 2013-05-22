"""
HappyBase Batch module.
"""

from collections import defaultdict
import logging
from numbers import Integral

from .hbase.ttypes import BatchMutation, Mutation

logger = logging.getLogger(__name__)


class Batch(object):
    """Batch mutation class.

    This class cannot be instantiated directly; use :py:meth:`Table.batch`
    instead.
    """
    def __init__(self, table, timestamp=None, batch_size=None,
                 transaction=False):
        """Initialise a new Batch instance."""
        if not (timestamp is None or isinstance(timestamp, Integral)):
            raise TypeError("'timestamp' must be an integer or None")

        if batch_size is not None:
            if transaction:
                raise TypeError("'transaction' cannot be used when "
                                "'batch_size' is specified")
            if not batch_size > 0:
                raise ValueError("'batch_size' must be > 0")

        self._table = table
        self._batch_size = batch_size
        self._timestamp = timestamp
        self._transaction = transaction
        self._families = None
        self._reset_mutations()

    def _reset_mutations(self):
        """Reset the internal mutation buffer."""
        self._mutations = defaultdict(list)
        self._mutation_count = 0

    def send(self):
        """Send the batch to the server."""
        bms = [BatchMutation(row, m) for row, m in self._mutations.iteritems()]
        if not bms:
            return

        logger.debug("Sending batch for '%s' (%d mutations on %d rows)",
                     self._table.name, self._mutation_count, len(bms))
        if self._timestamp is None:
            self._table.connection.client.mutateRows(self._table.name, bms)
        else:
            self._table.connection.client.mutateRowsTs(
                self._table.name, bms, self._timestamp)

        self._reset_mutations()

    #
    # Mutation methods
    #

    def put(self, row, data):
        """Store data in the table.

        See :py:meth:`Table.put` for a description of the `row` and `data`
        arguments.
        """
        self._mutations[row].extend(
            Mutation(isDelete=False, column=column, value=value)
            for column, value in data.iteritems())

        self._mutation_count += len(data)
        if self._batch_size and self._mutation_count >= self._batch_size:
            self.send()

    def delete(self, row, columns=None):
        """Delete data from the table.

        See :py:meth:`Table.delete` for a description of the `row` and
        `columns` arguments.
        """
        # Work-around Thrift API limitation: the mutation API can only
        # delete specified columns, not complete rows, so just list the
        # column families once and cache them for later use in the same
        # transaction.
        if columns is None:
            if self._families is None:
                self._families = self._table._column_family_names()
            columns = self._families

        self._mutations[row].extend(
            Mutation(isDelete=True, column=column) for column in columns)

        self._mutation_count += len(columns)
        if self._batch_size and self._mutation_count >= self._batch_size:
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
        if self._transaction and exc_type is not None:
            return

        self.send()
