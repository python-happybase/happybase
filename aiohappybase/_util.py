"""
AIOHappyBase utility module.

These functions are not part of the public API.
"""

import re
from typing import Dict, List, Any, AnyStr, Optional, TypeVar, Callable

T = TypeVar('T')

KTI = TypeVar('KTI')
VTI = TypeVar('VTI')

KTO = TypeVar('KTO')
VTO = TypeVar('VTO')

CAPITALS = re.compile('([A-Z])')


def camel_case_to_pep8(name: str) -> str:
    """Convert a camel cased name to PEP8 style."""
    converted = CAPITALS.sub(lambda m: '_' + m.groups()[0].lower(), name)
    return converted[1:] if converted[0] == '_' else converted


def pep8_to_camel_case(name: str, initial: bool = False) -> str:
    """Convert a PEP8 style name to camel case."""
    chunks = name.split('_')
    converted = [s.capitalize() for s in chunks]
    if initial:
        return ''.join(converted)
    else:
        return chunks[0].lower() + ''.join(converted[1:])


def thrift_attrs(obj_or_cls) -> List[str]:
    """Obtain Thrift data type attribute names for an instance or class."""
    return [v[1] for v in obj_or_cls.thrift_spec.values()]


def thrift_type_to_dict(obj: Any) -> Dict[bytes, Any]:
    """Convert a Thrift data type to a regular dictionary."""
    return {
        camel_case_to_pep8(attr): getattr(obj, attr)
        for attr in thrift_attrs(obj)
    }


def ensure_bytes(value: AnyStr) -> bytes:
    """Convert text into bytes, and leaves bytes as-is."""
    if isinstance(value, bytes):
        return value 
    if isinstance(value, str):
        return value.encode('utf-8')
    raise TypeError(
        f"input must be a text or byte string, got {type(value).__name__}"
    )


def bytes_increment(b: bytes) -> Optional[bytes]:
    """
    Increment and truncate a byte string (for sorting purposes)

    This functions returns the shortest string that sorts after the given
    string when compared using regular string comparison semantics.

    This function increments the last byte that is smaller than ``0xFF``, and
    drops everything after it. If the string only contains ``0xFF`` bytes,
    `None` is returned.
    """
    assert isinstance(b, bytes)
    b = bytearray(b)  # Used subset of its API is the same on Python 2 and 3.
    for i in range(len(b) - 1, -1, -1):
        if b[i] != 0xff:
            b[i] += 1
            return bytes(b[:i+1])
    return None


def _id(x: T) -> T: return x


def map_dict(data: Dict[KTI, VTI],
             keys: Callable[[KTI], KTO] = _id,
             values: Callable[[VTI], VTO] = _id) -> Dict[KTO, VTO]:
    """
    Dictionary mapping function, analogous to :py:func:`builtins.map`. Allows
    applying a specific function independently to both the keys and values.

    :param data: Dictionary to apply mapping to
    :param keys: Optional function to apply to all keys
    :param values: Optional function to apply to all values
    :return: New dictionary with keys and values mapped
    """
    return {keys(k): values(v) for k, v in data.items()}
