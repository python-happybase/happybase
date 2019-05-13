"""
HappyBase, a developer-friendly Python library to interact with Apache
HBase.
"""

import pkg_resources as _pkg_resources
import thriftpy2 as _thriftpy
_thriftpy.load(
    _pkg_resources.resource_filename('happybase', 'Hbase.thrift'),
    'Hbase_thrift')

from ._version import __version__  # noqa

from .connection import DEFAULT_HOST, DEFAULT_PORT, Connection  # noqa
from .table import Table  # noqa
from .batch import Batch  # noqa
from .pool import ConnectionPool, NoConnectionsAvailable  # noqa
