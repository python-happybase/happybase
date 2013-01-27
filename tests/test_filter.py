"""
HappyBase filter tests.
"""

from __future__ import unicode_literals

from nose.tools import assert_equal, assert_raises

from happybase.filter import escape


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
