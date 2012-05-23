"""
HappyBase utility tests.
"""

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


def test_str_increment():
    def check(s_hex, expected):
        s = s_hex.decode('hex')
        v = util.str_increment(s)
        v_hex = v.encode('hex')
        assert_equal(expected, v_hex)
        assert_less(s, v)

    test_values = [
        ('00', '01'),
        ('01', '02'),
        ('fe', 'ff'),
        ('1234', '1235'),
        ('12fe', '12ff'),
        ('12ff', '13'),
        ('424242ff', '424243'),
        ('4242ffff', '4243'),
    ]

    assert util.str_increment('\xff\xff\xff') is None

    for s, expected in test_values:
        yield check, s, expected
