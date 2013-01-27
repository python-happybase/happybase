"""
Filter module.

This module provides helper routines to construct Thrift filter strings.
"""

def escape(s):
    """Escape a byte string for use in a filter string

    :param str host: The byte string to escape
    :return: Escaped string
    :rtype: str
    """

    if not isinstance(s, bytes):
        raise TypeError("Only byte strings can be escaped")

    return s.replace("'", "''")
