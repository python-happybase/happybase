from happybase.hbase.ttypes import TIncrement
from collections import defaultdict


class CounterBatch(object):
    def __init__(self, table, batch_size=None):
        self.table = table
        self.batch_size = batch_size
        self.batch = defaultdict(int)
        self.batch_count = 0

    def counter_inc(self, row, column, value=1):
        self.batch[(row, column)] += value
        self.batch_count += 1
        self._check_send()

    def counter_dec(self, row, column, value=1):
        self.counter_inc(row, column, -value)

    def send(self):
        increment_rows = [
            TIncrement(table=self.table.name, row=key[0], column=key[1], ammount=value)
            for key, value in self.batch.iteritems()
        ]
        self.table.connection.client.incrementRows(increment_rows)
        self.batch.clear()
        self.batch_count = 0

    def _check_send(self):
        if self.batch_size and (self.batch_count >= self.batch_size):
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
