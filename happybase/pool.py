"""
HappyBase connection pool module.
"""

import contextlib
import logging
import Queue
import threading

from .connection import Connection

logger = logging.getLogger(__name__)

#
# TODO: maybe support multiple Thrift servers. What would a reasonable
# distribution look like? Round-robin? Randomize the list upon
# instantiation and then cycle through it? How to handle (temporary?)
# connection errors?
#


class _ClientProxy(object):
    """
    Proxy class to silently notice Thrift client exceptions.

    This class proxies all requests a Connection makes to the underlying
    Thrift client, and sets a flag when the client raised an exception,
    e.g. socket errors or Thrift protocol errors.

    The connection pool replaces tainted connections with fresh ones.
    """
    def __init__(self, connection, client):
        self.connection = connection
        self.client = client
        self._cache = {}

    def __getattr__(self, name):
        """
        Hook into attribute lookup and return wrapped methods.

        Since the client is only used for method calls, just treat
        every attribute access as a method to wrap.
        """
        wrapped = self._cache.get(name)

        if wrapped is None:
            def wrapped(*args, **kwargs):
                method = getattr(self.client, name)
                assert callable(method)
                try:
                    return method(*args, **kwargs)
                except:
                    self.connection._tainted = True
                    raise
            self._cache[name] = wrapped

        return wrapped


class NoConnectionsAvailable(RuntimeError):
    """
    Exception raised when no connections are available.

    This happens if a timeout was specified when obtaining a connection,
    and no connection became available within the specified timeout.

    .. versionadded:: 0.5
    """
    pass


class ConnectionPool(object):
    """
    Thread-safe connection pool.

    .. versionadded:: 0.5

    The `size` parameter specifies how many connections this pool
    manages. Additional keyword arguments are passed unmodified to the
    :py:class:`happybase.Connection` constructor, with the exception of
    the `autoconnect` argument, since maintaining connections is the
    task of the pool.

    :param int size: the maximum number of concurrently open connections
    :param kwargs: keyword arguments passed to
                   :py:class:`happybase.Connection`
    """
    def __init__(self, size, **kwargs):
        if not isinstance(size, int):
            raise TypeError("Pool 'size' arg must be an integer")

        if not size > 0:
            raise ValueError("Pool 'size' arg must be greater than zero")

        logger.debug(
            "Initializing connection pool with %d connections", size)

        self._lock = threading.Lock()
        self._queue = Queue.LifoQueue(maxsize=size)
        self._thread_connections = threading.local()

        self._connection_kwargs = kwargs
        self._connection_kwargs['autoconnect'] = False

        for i in xrange(size):
            connection = self._create_connection()
            self._queue.put(connection)

        # The first connection is made immediately so that trivial
        # mistakes like unresolvable host names are raised immediately.
        # Subsequent connections are connected lazily.
        with self.connection():
            pass

    def _create_connection(self):
        """Create a new connection with monkey-patched Thrift client."""
        connection = Connection(**self._connection_kwargs)
        connection.client = _ClientProxy(connection, connection.client)
        return connection

    def _acquire_connection(self, timeout=None):
        """Acquire a connection from the pool."""
        try:
            return self._queue.get(True, timeout)
        except Queue.Empty:
            raise NoConnectionsAvailable(
                "No connection available from pool within specified "
                "timeout")

    def _return_connection(self, connection):
        """Return a connection to the pool."""
        self._queue.put(connection)

    @contextlib.contextmanager
    def connection(self, timeout=None):
        """
        Obtain a connection from the pool.

        This method *must* be used as a context manager, i.e. with
        Python's ``with`` block. Example::

            with pool.connection() as connection:
                pass  # do something with the connection

        If `timeout` is specified, this is the number of seconds to wait
        for a connection to become available before
        :py:exc:`NoConnectionsAvailable` is raised. If omitted, this
        method waits forever for a connection to become available.

        :param int timeout: number of seconds to wait (optional)
        :return: active connection from the pool
        :rtype: :py:class:`happybase.Connection`
        """

        # If this thread already holds a connection, just return it.
        # This is the short path for nested calls from the same thread.
        connection = getattr(self._thread_connections, 'current', None)
        if connection is not None:
            yield connection
            return

        # If this point is reached, this is the outermost connection
        # requests for a thread. Obtain a new connection from the pool
        # and keep a reference in a thread local so that nested
        # connection requests from the same thread can return the same
        # connection instance.
        #
        # Note: this code acquires a lock before assigning to the
        # thread local; see
        # http://emptysquare.net/blog/another-thing-about-pythons-
        # threadlocals/

        connection = self._acquire_connection(timeout)
        with self._lock:
            self._thread_connections.current = connection

        try:
            # Open connection, because connections are opened lazily.
            # This is a no-op for connections that are already open.
            connection.open()

            # Return value from the context manager's __enter__()
            yield connection

        finally:

            # Remove thread local reference, since the thread no longer
            # owns it.
            del self._thread_connections.current

            # Refresh the underlying Thrift client if an exception
            # occurred in the Thrift layer, since we don't know whether
            # the connection is still usable.
            if getattr(connection, '_tainted', False):
                logger.info("Replacing tainted pool connection")

                try:
                    connection.close()
                except:
                    pass

                connection = self._create_connection()

            self._return_connection(connection)
