"""
HappyBase filter tests.
"""

from __future__ import unicode_literals

from nose.tools import assert_equal, assert_raises

import happybase.filter as filter
from happybase.filter import escape, make_filter, QualifierFilter


def test_escape():

    assert_raises(TypeError, escape, u'foo')
    assert_raises(TypeError, escape, 42)
    assert_raises(TypeError, escape, None)

    def check(original, expected):
        actual = escape(original)
        assert_equal(actual, expected)

    test_values = [
        (b'', b''),
        (b'foo', b'foo'),
        (b'\x03\x02\x01\x00', b'\x03\x02\x01\x00'),
        (b"foo'ba''r", b"foo''ba''''r"),
    ]

    for original, expected in test_values:
        yield check, original, expected


def test_serialization():

    # Comparison operators
    f = QualifierFilter(
        filter.LESS,
        filter.LESS_OR_EQUAL,
        filter.EQUAL,
        filter.NOT_EQUAL,
        filter.GREATER_OR_EQUAL,
        filter.GREATER,
    )
    exp = b"QualifierFilter(<, <=, =, !=, >=, >)"
    assert_equal(exp, bytes(f))

    # Booleans
    f = QualifierFilter(True, False)
    exp = b"QualifierFilter(true, false)"
    assert_equal(exp, bytes(f))

    # Integers
    f = QualifierFilter(12, 13, -1, 0)
    exp = b"QualifierFilter(12, 13, -1, 0)"
    assert_equal(exp, bytes(f))

    # Strings
    f = QualifierFilter(b'foo', b"foo'bar", b'bar')
    exp = b"QualifierFilter('foo', 'foo''bar', 'bar')"
    assert_equal(exp, bytes(f))

    # Mixed args
    assert_equal(
        b"QualifierFilter(>=, 'foo', 12, 'bar')",
        bytes(QualifierFilter(filter.GREATER_OR_EQUAL, b'foo', 12, b'bar'))
    )


def test_type_checking():
    assert_raises(TypeError, QualifierFilter, u'foo')
    assert_raises(TypeError, QualifierFilter, 3.14)
    assert_raises(TypeError, QualifierFilter, object())
    assert_raises(TypeError, QualifierFilter, None)


def test_custom_filter():

    MyCustomFilter = make_filter('MyCustomFilter')

    assert_equal(
        b"MyCustomFilter(1, =, 'foo''bar')",
        bytes(MyCustomFilter(1, filter.EQUAL, b"foo'bar"))
    )

    with assert_raises(TypeError):
        f = make_filter(None)
        f(1, 2)

    with assert_raises(TypeError):
        f = make_filter, (12)
        f(1, 2)
