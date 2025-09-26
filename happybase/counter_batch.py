import six
from Hbase_thrift import Hbase, ColumnDescriptor, TIncrement
from collections import defaultdict


class CounterBatch(object):
    def __init__(self, table, batch_size=None):
        self.table = table
        self.batch_size = batch_size
        self.batch = defaultdict(int)

    def counter_inc(self, row, column, value=1):
        self.batch[(row, column)] += value
        self._check_send()

    def counter_dec(self, row, column, value=1):
        self.counter_inc(row, column, -value)

    def send(self):
        increment_rows = [
            TIncrement(table=self.table.name, row=key[0], column=key[1], ammount=value)
            for key, value in six.iteritems(self.batch)
        ]
        self.table.connection.client.incrementRows(increment_rows)
        self.batch.clear()

    def _check_send(self):
        if len(self.batch) >= self.batch_size:
            self.send()

    #
    # Context manager methods
    #

    def __enter__(self):
        """Called upon entering a ``with`` block"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Called upon exiting a ``with`` block"""
        # TODO: Examine the exception and decide whether or not to send
        # For now we always send
        if exc_type is not None:
            pass

        self.send()
