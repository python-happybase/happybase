"""
Filter module.

This module provides helper routines to construct Thrift filter strings.
"""

# TODO: support AND, OR, WHILE, SKIP (with operator overloading?)

from __future__ import unicode_literals as _unicode_literals
from functools import partial as _partial


LESS = LT = object()
LESS_OR_EQUAL = LE = object()
EQUAL = EQ = object()
NOT_EQUAL = NE = object()
GREATER_OR_EQUAL = GE = object()
GREATER = GT = object()
NO_OP = object()

_COMPARISON_OPERATOR_STRINGS = {
    LESS: '<',
    LESS_OR_EQUAL: '<=',
    EQUAL: '=',
    NOT_EQUAL: '!=',
    GREATER_OR_EQUAL: '>=',
    GREATER: '>',
    NO_OP: '',
}


def escape(s):
    """Escape a byte string for use in a filter string.

    :param str host: The byte string to escape
    :return: Escaped string
    :rtype: str
    """

    if not isinstance(s, bytes):
        raise TypeError("Only byte strings can be escaped")

    return s.replace(b"'", b"''")


class _Filter:
    """Client-side Filter representation.

    This class does not have any filtering logic; it is only used to
    build filter strings that the HBase Thrift server can parse and
    apply.
    """
    def __init__(self, name, *args):

        if isinstance(name, unicode):
            name = name.encode('ascii')

        if not isinstance(name, bytes):
            raise TypeError("Filter name must be a string")

        self.name = name
        self.args = map(self._format_arg, args)

    def _format_arg(self, arg):
        if isinstance(arg, int):
            return bytes(arg)

        if arg in _COMPARISON_OPERATOR_STRINGS:
            return _COMPARISON_OPERATOR_STRINGS[arg]

        if isinstance(arg, bytes):
            # TODO: what to do with already escaped strings?
            return "'%s'" % escape(arg)

        raise TypeError(
            "Filter arguments must be integers, comparison operators "
            "or byte strings; got %r" % arg)

    def __str__(self):
        return b'%s(%s)' % (self.name, ', '.join(self.args))


def make_filter(name):
    """Define a new filter with the specified name.

    Use this function to specify custom filters that are not included by
    default, such as custom filters you wrote yourself and made
    available in the HBase server (or newly added filters that are not
    yet in HappyBase).

    The callable returned by this function can be used just like the
    built-in filters.

    Example::

       MyCustomFilter = make_filter(b'MyCustomFilter')
       f = MyCustomFilter(1, b'foo')
       table.scan(..., filter=f)

    :param str name: name of the filter
    :return: new filter callable
    :rtype: filter callable
    """
    return _partial(_Filter, name)


#
# Built-in filters (taken from the Thrift docs)
#

KeyOnlyFilter = make_filter('KeyOnlyFilter')
FirstKeyOnlyFilter = make_filter('FirstKeyOnlyFilter')
PrefixFilter = make_filter('PrefixFilter')
ColumnPrefixFilter = make_filter('ColumnPrefixFilter')
MultipleColumnPrefixFilter = make_filter('MultipleColumnPrefixFilter')
ColumnCountGetFilter = make_filter('ColumnCountGetFilter')
PageFilter = make_filter('PageFilter')
ColumnPaginationFilter = make_filter('ColumnPaginationFilter')
InclusiveStopFilter = make_filter('InclusiveStopFilter')
TimeStampsFilter = make_filter('TimeStampsFilter')
RowFilter = make_filter('RowFilter')
FamilyFilter = make_filter('FamilyFilter')
QualifierFilter = make_filter('QualifierFilter')
QualifierFilter = make_filter('QualifierFilter')
ValueFilter = make_filter('ValueFilter')
DependentColumnFilter = make_filter('DependentColumnFilter')
SingleColumnValueFilter = make_filter('SingleColumnValueFilter')
SingleColumnValueExcludeFilter = make_filter('SingleColumnValueExcludeFilter')
ColumnRangeFilter = make_filter('ColumnRangeFilter')
