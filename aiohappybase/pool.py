"""
AIOHappyBase connection pool module.
"""

import logging
import socket
import asyncio as aio
from numbers import Real

from thriftpy2.thrift import TException

from .connection import Connection

try:
    from asyncio import current_task
except ImportError:  # < 3.7
    current_task = aio.Task.current_task

try:
    from contextlib import asynccontextmanager
except ImportError:  # < 3.7
    from async_generator import asynccontextmanager

logger = logging.getLogger(__name__)

#
# TODO: maybe support multiple Thrift servers. What would a reasonable
# distribution look like? Round-robin? Randomize the list upon
# instantiation and then cycle through it? How to handle (temporary?)
# connection errors?
#


class NoConnectionsAvailable(RuntimeError):
    """
    Exception raised when no connections are available.

    This happens if a timeout was specified when obtaining a connection,
    and no connection became available within the specified timeout.

    .. versionadded:: 0.5
    """
    pass


class ConnectionPool:
    """
    Asyncio-safe connection pool.

    .. versionadded:: 0.5

    Connection pools in sync code (like :py:class:`happybase.ConnectionPool`)
    work by creating multiple connections and providing one whenever a thread
    asks. When a thread is done with it, it returns it too the pool to be
    made available to other threads. In async code, instead of threads,
    tasks make the request to the pool for a connection.

    If a task nests calls to :py:meth:`connection`, it will get the
    same connection back, just like in HappyBase.

    The `size` argument specifies how many connections this pool
    manages. Additional keyword arguments are passed unmodified to the
    :py:class:`happybase.Connection` constructor, with the exception of
    the `autoconnect` argument, since maintaining connections is the
    task of the pool.

    :param int size: the maximum number of concurrently open connections
    :param kwargs: keyword arguments for :py:class:`happybase.Connection`
    """
    def __init__(self, size: int, **kwargs):
        if not isinstance(size, int):
            raise TypeError("Pool 'size' arg must be an integer")

        if not size > 0:
            raise ValueError("Pool 'size' arg must be greater than zero")

        logger.debug(f"Initializing connection pool with {size} connections")

        self._queue = aio.LifoQueue(maxsize=size)
        self._task_connections = {}

        kwargs['autoconnect'] = False

        for i in range(size):
            self._queue.put_nowait(Connection(**kwargs))

    async def close(self):
        """Clean up all pool connections and delete the queue."""
        while True:
            try:
                await self._queue.get_nowait().close()
            except aio.QueueEmpty:
                break
        del self._queue

    async def _acquire_connection(self, timeout: Real = None) -> Connection:
        """Acquire a connection from the pool."""
        try:
            return await aio.wait_for(self._queue.get(), timeout)
        except aio.TimeoutError:
            raise NoConnectionsAvailable("Timeout waiting for a connection")

    async def _return_connection(self, connection: Connection) -> None:
        """Return a connection to the pool."""
        await self._queue.put(connection)

    @asynccontextmanager
    async def connection(self, timeout: Real = None) -> Connection:
        """
        Obtain a connection from the pool.

        This method *must* be used as a context manager, i.e. with
        Python's ``with`` block. Example::

            async with pool.connection() as connection:
                pass  # do something with the connection

        If `timeout` is specified, this is the number of seconds to wait
        for a connection to become available before
        :py:exc:`NoConnectionsAvailable` is raised. If omitted, this
        method waits forever for a connection to become available.

        :param timeout: number of seconds to wait (optional)
        :return: active connection from the pool
        :rtype: :py:class:`happybase.Connection`
        """
        task_id = id(current_task())
        connection = self._task_connections.get(task_id)

        return_after_use = False
        if connection is None:
            # This is the outermost connection requests for this task.
            # Obtain a new connection from the pool and keep a reference
            # by the task id so that nested calls get the same connection
            return_after_use = True
            connection = await self._acquire_connection(timeout)
            self._task_connections[task_id] = connection

        try:
            # Open connection, because connections are opened lazily.
            # This is a no-op for connections that are already open.
            await connection.open()

            # Return value from the context manager's __enter__()
            yield connection

        except (TException, socket.error):
            # Refresh the underlying Thrift client if an exception
            # occurred in the Thrift layer, since we don't know whether
            # the connection is still usable.
            logger.info("Replacing tainted pool connection")
            connection._refresh_thrift_client()
            await connection.open()

            # Reraise to caller; see contextlib.contextmanager() docs
            raise

        finally:
            # Remove thread local reference after the outermost 'with'
            # block ends. Afterwards the thread no longer owns the
            # connection.
            if return_after_use:
                del self._task_connections[task_id]
                await self._return_connection(connection)

    # Support async context usage
    async def __aenter__(self) -> 'ConnectionPool':
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.close()

    # Support context usage
    def __enter__(self) -> 'ConnectionPool':
        if aio.get_event_loop().is_running():
            raise RuntimeError("Use async with inside a running event loop!")
        return self

    def __exit__(self, *_exc) -> None:
        aio.get_event_loop().run_until_complete(self.close())

    def __del__(self) -> None:
        if hasattr(self, '_queue'):
            logger.warning(f"{self} was not closed!")
