from happybase.hbase.ttypes import TIncrement


class CounterBatch(object):
    def __init__(self, table, batch_size=None):
        self.table = table
        self.batch_size = batch_size
        self.batch = []

    def counter_inc(self, row, column, value=1):
        self.batch.append({'row': row, 'column': column, 'value': value})
        self._check_send()

    def counter_dec(self, row, column, value=1):
        self.batch.append({'row': row, 'column': column, 'value': -value})
        self._check_send()

    def send(self):
        increment_rows = []
        for increment in self.batch:
            increment_rows.append(
                TIncrement(
                    table=self.table.name,
                    row=increment['row'],
                    column=increment['column'],
                    ammount=increment.get('value', 1),
                )
            )
        self.table.connection.client.incrementRows(increment_rows)

    def _check_send(self):
        if self.batch_size and (len(self.batch) >= self.batch_size):
            self.send()
            self.batch = []

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
