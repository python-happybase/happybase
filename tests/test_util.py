"""
HappyBase utility tests.
"""

from nose.tools import assert_equal

import happybase.util


def test_camel_case_to_pep8():
    def check(lower_cc, upper_cc, correct):

        x1 = happybase.util.camel_case_to_pep8(lower_cc)
        x2 = happybase.util.camel_case_to_pep8(upper_cc)
        assert_equal(correct, x1)
        assert_equal(correct, x2)

        y1 = happybase.util.pep8_to_camel_case(x1, True)
        y2 = happybase.util.pep8_to_camel_case(x2, False)
        assert_equal(upper_cc, y1)
        assert_equal(lower_cc, y2)

    examples = [('foo', 'Foo', 'foo'),
                ('fooBar', 'FooBar', 'foo_bar'),
                ('fooBarBaz', 'FooBarBaz', 'foo_bar_baz'),
                ('fOO', 'FOO', 'f_o_o')]

    for a, b, c in examples:
        yield check, a, b, c
