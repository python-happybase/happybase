"""
HappyBase, a developer-friendly Python library to interact with Apache
HBase.
"""

import thriftpy as _thriftpy
_thriftpy.install_import_hook()

from ._version import __version__

from .connection import DEFAULT_HOST, DEFAULT_PORT, Connection
from .table import Table
from .batch import Batch
from .pool import ConnectionPool, NoConnectionsAvailable
