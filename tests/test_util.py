"""
HappyBase utility tests.
"""

from codecs import decode, encode

from nose.tools import assert_equal, assert_less

import happybase.util as util


def test_camel_case_to_pep8():
    def check(lower_cc, upper_cc, correct):

        x1 = util.camel_case_to_pep8(lower_cc)
        x2 = util.camel_case_to_pep8(upper_cc)
        assert_equal(correct, x1)
        assert_equal(correct, x2)

        y1 = util.pep8_to_camel_case(x1, True)
        y2 = util.pep8_to_camel_case(x2, False)
        assert_equal(upper_cc, y1)
        assert_equal(lower_cc, y2)

    examples = [('foo', 'Foo', 'foo'),
                ('fooBar', 'FooBar', 'foo_bar'),
                ('fooBarBaz', 'FooBarBaz', 'foo_bar_baz'),
                ('fOO', 'FOO', 'f_o_o')]

    for a, b, c in examples:
        yield check, a, b, c


def test_bytes_increment():
    def check(s_hex, expected):
        s = decode(s_hex, 'hex')
        v = util.bytes_increment(s)
        v_hex = encode(v, 'hex')
        assert_equal(expected, v_hex)
        assert_less(s, v)

    test_values = [
        (b'00', b'01'),
        (b'01', b'02'),
        (b'fe', b'ff'),
        (b'1234', b'1235'),
        (b'12fe', b'12ff'),
        (b'12ff', b'13'),
        (b'424242ff', b'424243'),
        (b'4242ffff', b'4243'),
    ]

    assert util.bytes_increment(b'\xff\xff\xff') is None

    for s, expected in test_values:
        yield check, s, expected
