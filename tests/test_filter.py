"""
HappyBase filter tests.
"""

from __future__ import unicode_literals

from nose.tools import assert_equal, assert_raises

from happybase.filter import (
    AND,
    EQUAL,
    escape,
    GREATER,
    GREATER_OR_EQUAL,
    LESS,
    LESS_OR_EQUAL,
    make_filter,
    NOT_EQUAL,
    OR,
    SKIP,
    ValueFilter,
    WHILE,
)


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
    f = ValueFilter(
        LESS,
        LESS_OR_EQUAL,
        EQUAL,
        NOT_EQUAL,
        GREATER_OR_EQUAL,
        GREATER,
    )
    exp = b"ValueFilter(<, <=, =, !=, >=, >)"
    assert_equal(exp, bytes(f))

    # Booleans
    f = ValueFilter(True, False)
    exp = b"ValueFilter(true, false)"
    assert_equal(exp, bytes(f))

    # Integers
    f = ValueFilter(12, 13, -1, 0)
    exp = b"ValueFilter(12, 13, -1, 0)"
    assert_equal(exp, bytes(f))

    # Strings
    f = ValueFilter(b'foo', b"foo'bar", b'bar')
    exp = b"ValueFilter('foo', 'foo''bar', 'bar')"
    assert_equal(exp, bytes(f))

    # Mixed args
    assert_equal(
        b"ValueFilter(>=, 'foo', 12, 'bar')",
        bytes(ValueFilter(GREATER_OR_EQUAL, b'foo', 12, b'bar'))
    )


def test_type_checking():
    assert_raises(TypeError, ValueFilter, u'foo')
    assert_raises(TypeError, ValueFilter, 3.14)
    assert_raises(TypeError, ValueFilter, object())
    assert_raises(TypeError, ValueFilter, None)


def test_custom_filter():

    MyCustomFilter = make_filter('MyCustomFilter')

    assert_equal(
        b"MyCustomFilter(1, =, 'foo''bar')",
        bytes(MyCustomFilter(1, EQUAL, b"foo'bar"))
    )

    with assert_raises(TypeError):
        f = make_filter(None)
        f(1, 2)

    with assert_raises(TypeError):
        f = make_filter, (12)
        f(1, 2)


def test_unary_operators():

    assert_equal(
        b'SKIP (ValueFilter())',
        bytes(SKIP(ValueFilter()))
    )

    assert_equal(
        b'WHILE (ValueFilter())',
        bytes(WHILE(ValueFilter()))
    )


def test_binary_operators():

    def check(expected, original):
        actual = bytes(original)
        assert_equal(actual, expected)

    f = b"(ValueFilter('foo') AND ValueFilter('bar'))"
    check(f, AND(ValueFilter(b'foo'), ValueFilter(b'bar')))
    check(f, ValueFilter(b'foo') & ValueFilter(b'bar'))

    f = b"(ValueFilter('foo') OR ValueFilter('bar'))"
    check(f, OR(ValueFilter(b'foo'), ValueFilter(b'bar')))
    check(f, ValueFilter(b'foo') | ValueFilter(b'bar'))
