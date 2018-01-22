"""
Happybase key-value store module.
"""

from collections.abc import MutableMapping
from warnings import warn

class Map(MutableMapping):
    """
    Key-value store on top of HBase.
    """

    def __init__(self, connection, table):
        self._table = connection.table(table)
        self._key_charset = 'ascii'

    def __setitem__(self, key, value):
        self._table.put(key.encode(self._key_charset), {b'cf:v': value})

    def __getitem__(self, key):
        row = self._table.row(key.encode(self._key_charset))
        return row[b'cf:v']

    def __delitem__(self, key):
        self._table.delete(key.encode(self._key_charset))

    def __iter__(self):
        warn("Calling happybase.Map.__iter__. Scanning all rows.")

        for key, value in self._table.scan():
            yield key.decode(self._key_charset)

    def __len__(self):
        warn("Calling happybase.Map.__len__. Scanning all rows.")

        return sum((1 for key, value in self._table.scan()))
