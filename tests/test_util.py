"""
HappyBase utility tests.
"""

from codecs import decode, encode
import unittest as ut

from aiohappybase import _util as util  # noqa


class TestUtil(ut.TestCase):
    def test_camel_case_to_pep8(self):
        examples = [
            ('foo', 'Foo', 'foo'),
            ('fooBar', 'FooBar', 'foo_bar'),
            ('fooBarBaz', 'FooBarBaz', 'foo_bar_baz'),
            ('fOO', 'FOO', 'f_o_o'),
        ]

        for lower_cc, upper_cc, correct in examples:
            x1 = util.camel_case_to_pep8(lower_cc)
            x2 = util.camel_case_to_pep8(upper_cc)
            self.assertEqual(correct, x1)
            self.assertEqual(correct, x2)

            y1 = util.pep8_to_camel_case(x1, True)
            y2 = util.pep8_to_camel_case(x2, False)
            self.assertEqual(upper_cc, y1)
            self.assertEqual(lower_cc, y2)

    def test_bytes_increment(self):
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

        self.assertIsNone(util.bytes_increment(b'\xff\xff\xff'))

        for s, expected in test_values:
            s = decode(s, 'hex')
            v = util.bytes_increment(s)
            v_hex = encode(v, 'hex')
            self.assertEqual(expected, v_hex)
            self.assertLess(s, v)
