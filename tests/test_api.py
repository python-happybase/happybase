"""
HappyBase tests.
"""

import os
import random
import threading
import asyncio as aio
from typing import AsyncGenerator

import asynctest

from aiohappybase import (
    Connection,
    Table,
    ConnectionPool,
    NoConnectionsAvailable,
)

AIOHAPPYBASE_HOST = os.environ.get('AIOHAPPYBASE_HOST', 'localhost')
AIOHAPPYBASE_PORT = int(os.environ.get('AIOHAPPYBASE_PORT', '9090'))
AIOHAPPYBASE_COMPAT = os.environ.get('AIOHAPPYBASE_COMPAT', '0.98')
AIOHAPPYBASE_TRANSPORT = os.environ.get('AIOHAPPYBASE_TRANSPORT', 'buffered')

TABLE_PREFIX = b'happybase_tests_tmp'
TEST_TABLE_NAME = b'test1'

connection_kwargs = dict(
    host=AIOHAPPYBASE_HOST,
    port=AIOHAPPYBASE_PORT,
    table_prefix=TABLE_PREFIX,
    compat=AIOHAPPYBASE_COMPAT,
    transport=AIOHAPPYBASE_TRANSPORT,
)


class TestAPI(asynctest.TestCase):
    use_default_loop = True

    connection: Connection
    table: Table

    @classmethod
    def setUpClass(cls):
        loop = aio.get_event_loop()
        run = loop.run_until_complete

        conn = cls.connection = Connection(**connection_kwargs)
        assert cls.connection is not None

        run(conn.open())

        tables = run(conn.tables())
        if TEST_TABLE_NAME in tables:
            print("Test table already exists; removing it...")
            run(conn.delete_table(TEST_TABLE_NAME, disable=True))

        cfs = {
            'cf1': {},
            'cf2': None,
            'cf3': {'max_versions': 1},
        }
        cls.table = run(conn.create_table(TEST_TABLE_NAME, families=cfs))
        assert cls.table is not None

    @classmethod
    def tearDownClass(cls):
        loop = aio.get_event_loop()
        loop.run_until_complete(
            cls.connection.delete_table(TEST_TABLE_NAME, disable=True)
        )
        cls.connection.close()
        del cls.connection
        del cls.table

    async def _scan_list(self, *args, **kwargs):
        return [x async for x in self.table.scan(*args, **kwargs)]

    async def _scan_len(self, scanner: AsyncGenerator = None, **kwargs) -> int:
        if scanner is None:
            scanner = self.table.scan(**kwargs)
        i = 0
        async for _ in scanner:
            i += 1
        return i

    def test_connection_compat(self):
        with self.assertRaises(ValueError):
            Connection(compat='0.1.invalid.version')

    def test_timeout_arg(self):
        Connection(timeout=5000)

    async def test_enabling(self):
        conn = self.connection
        self.assertTrue(await conn.is_table_enabled(TEST_TABLE_NAME))
        await conn.disable_table(TEST_TABLE_NAME)
        self.assertFalse(await conn.is_table_enabled(TEST_TABLE_NAME))
        await conn.enable_table(TEST_TABLE_NAME)
        self.assertTrue(await conn.is_table_enabled(TEST_TABLE_NAME))

    async def test_compaction(self):
        await self.connection.compact_table(TEST_TABLE_NAME)
        await self.connection.compact_table(TEST_TABLE_NAME, major=True)

    async def test_prefix(self):
        conn = self.connection
        self.assertEqual(TABLE_PREFIX + b'_', conn._table_name(''))
        self.assertEqual(TABLE_PREFIX + b'_foo', conn._table_name('foo'))

        self.assertEqual(conn.table('foobar').name, TABLE_PREFIX + b'_foobar')
        self.assertEqual(conn.table('foobar', use_prefix=False).name, b'foobar')

        c = Connection()
        self.assertEqual(b'foo', c._table_name('foo'))

        with self.assertRaises(TypeError):
            Connection(table_prefix=123)  # noqa

        with self.assertRaises(TypeError):
            Connection(table_prefix_separator=2.1)  # noqa

    async def test_stringify(self):
        str(self.connection)
        repr(self.connection)
        str(self.table)
        repr(self.table)

    async def test_table_listing(self):
        names = await self.connection.tables()
        self.assertIsInstance(names, list)
        self.assertIn(TEST_TABLE_NAME, names)

    async def test_table_regions(self):
        regions = await self.table.regions()
        self.assertIsInstance(regions, list)

    async def test_invalid_table_create(self):
        with self.assertRaises(ValueError):
            await self.connection.create_table('sometable', families={})
        with self.assertRaises(TypeError):
            await self.connection.create_table('sometable', families=0)  # noqa
        with self.assertRaises(TypeError):
            await self.connection.create_table('sometable', families=[])  # noqa

    async def test_families(self):
        families = await self.table.families()
        for name, fdesc in families.items():
            self.assertIsInstance(name, bytes)
            self.assertIsInstance(fdesc, dict)
            self.assertIn('name', fdesc)
            self.assertIsInstance(fdesc['name'], bytes)
            self.assertIn('max_versions', fdesc)

    async def test_put(self):
        await self.table.put(b'r1', {b'cf1:c1': b'v1',
                                     b'cf1:c2': b'v2',
                                     b'cf2:c3': b'v3'})
        await self.table.put(b'r1', {b'cf1:c4': b'v2'}, timestamp=2345678)
        await self.table.put(b'r1', {b'cf1:c4': b'v2'}, timestamp=1369168852994)

    async def test_atomic_counters(self):
        row = b'row-with-counter'
        column = b'cf1:counter'

        get = self.table.counter_get
        inc = self.table.counter_inc
        dec = self.table.counter_dec

        self.assertEqual(0, await get(row, column))

        self.assertEqual(10, await inc(row, column, 10))
        self.assertEqual(10, await get(row, column))

        await self.table.counter_set(row, column, 0)
        self.assertEqual(1, await inc(row, column))
        self.assertEqual(4, await inc(row, column, 3))
        self.assertEqual(4, await get(row, column))

        await self.table.counter_set(row, column, 3)
        self.assertEqual(3, await get(row, column))
        self.assertEqual(8, await inc(row, column, 5))
        self.assertEqual(6, await inc(row, column, -2))
        self.assertEqual(5, await dec(row, column))
        self.assertEqual(3, await dec(row, column, 2))
        self.assertEqual(10, await dec(row, column, -7))

    async def test_batch(self):
        with self.assertRaises(TypeError):
            self.table.batch(timestamp='invalid')  # noqa

        b = self.table.batch()
        await b.put(b'row1', {b'cf1:col1': b'value1',
                              b'cf1:col2': b'value2'})
        await b.put(b'row2', {b'cf1:col1': b'value1',
                              b'cf1:col2': b'value2',
                              b'cf1:col3': b'value3'})
        await b.delete(b'row1', [b'cf1:col4'])
        await b.delete(b'another-row')
        await b.close()

        self.table.batch(timestamp=1234567)
        await b.put(b'row1', {b'cf1:col5': b'value5'})
        await b.close()

        with self.assertRaises(ValueError):
            self.table.batch(batch_size=0)

        with self.assertRaises(TypeError):
            self.table.batch(transaction=True, batch_size=10)

    async def test_batch_context_managers(self):
        async with self.table.batch() as b:
            await b.put(b'row4', {b'cf1:col3': b'value3'})
            await b.put(b'row5', {b'cf1:col4': b'value4'})
            await b.put(b'row', {b'cf1:col1': b'value1'})
            await b.delete(b'row', [b'cf1:col4'])
            await b.put(b'row', {b'cf1:col2': b'value2'})

        async with self.table.batch(timestamp=87654321) as b:
            await b.put(b'row', {b'cf1:c3': b'somevalue',
                                 b'cf1:c5': b'anothervalue'})
            await b.delete(b'row', [b'cf1:c3'])

        with self.assertRaises(ValueError):
            async with self.table.batch(transaction=True) as b:
                await b.put(b'fooz', {b'cf1:bar': b'baz'})
                raise ValueError
        self.assertDictEqual({}, await self.table.row(b'fooz', [b'cf1:bar']))

        with self.assertRaises(ValueError):
            async with self.table.batch(transaction=False) as b:
                await b.put(b'fooz', {b'cf1:bar': b'baz'})
                raise ValueError
        self.assertDictEqual(
            {b'cf1:bar': b'baz'},
            await self.table.row(b'fooz', [b'cf1:bar']),
        )

        async with self.table.batch(batch_size=5) as b:
            for i in range(10):
                await b.put(
                    f'row-batch1-{i:03}'.encode('ascii'),
                    {b'cf1:': str(i).encode('ascii')},
                )

        async with self.table.batch(batch_size=20) as b:
            for i in range(95):
                await b.put(
                    f'row-batch2-{i:03}'.encode('ascii'),
                    {b'cf1:': str(i).encode('ascii')},
                )
        self.assertEqual(95, await self._scan_len(row_prefix=b'row-batch2-'))

        async with self.table.batch(batch_size=20) as b:
            for i in range(95):
                await b.delete(f'row-batch2-{i:03}'.encode('ascii'))
        self.assertEqual(0, await self._scan_len(row_prefix=b'row-batch2-'))

    async def test_row(self):
        row = self.table.row
        put = self.table.put
        row_key = b'row-test'

        with self.assertRaises(TypeError):
            await row(row_key, 123)  # noqa

        with self.assertRaises(TypeError):
            await row(row_key, timestamp='invalid')  # noqa

        await put(row_key, {b'cf1:col1': b'v1old'}, timestamp=1234)
        await put(row_key, {b'cf1:col1': b'v1new'}, timestamp=3456)
        await put(row_key, {b'cf1:col2': b'v2', b'cf2:col1': b'v3'})
        await put(row_key, {b'cf2:col2': b'v4'}, timestamp=1234)

        exp = {b'cf1:col1': b'v1new',
               b'cf1:col2': b'v2',
               b'cf2:col1': b'v3',
               b'cf2:col2': b'v4'}
        self.assertDictEqual(exp, await row(row_key))

        exp = {b'cf1:col1': b'v1new', b'cf1:col2': b'v2'}
        self.assertDictEqual(exp, await row(row_key, [b'cf1']))

        exp = {b'cf1:col1': b'v1new', b'cf2:col2': b'v4'}
        self.assertDictEqual(exp, await row(row_key, list(exp)))

        exp = {b'cf1:col1': b'v1old', b'cf2:col2': b'v4'}
        self.assertDictEqual(exp, await row(row_key, timestamp=2345))

        self.assertDictEqual({}, await row(row_key, timestamp=123))

        res = await row(row_key, include_timestamp=True)
        self.assertEqual(len(res), 4)
        self.assertEqual(b'v1new', res[b'cf1:col1'][0])
        self.assertIsInstance(res[b'cf1:col1'][1], int)

    async def test_rows(self):
        row_keys = [b'rows-row1', b'rows-row2', b'rows-row3']
        data_old = {b'cf1:col1': b'v1old', b'cf1:col2': b'v2old'}
        data_new = {b'cf1:col1': b'v1new', b'cf1:col2': b'v2new'}

        with self.assertRaises(TypeError):
            await self.table.rows(row_keys, object())  # noqa

        with self.assertRaises(TypeError):
            await self.table.rows(row_keys, timestamp='invalid')  # noqa

        for row_key in row_keys:
            await self.table.put(row_key, data_old, timestamp=4000)

        for row_key in row_keys:
            await self.table.put(row_key, data_new)

        self.assertDictEqual({}, dict(await self.table.rows([])))

        rows = dict(await self.table.rows(row_keys))
        for row_key in row_keys:
            self.assertIn(row_key, rows)
            self.assertDictEqual(data_new, rows[row_key])

        rows = dict(await self.table.rows(row_keys, timestamp=5000))
        for row_key in row_keys:
            self.assertIn(row_key, rows)
            self.assertDictEqual(data_old, rows[row_key])

    async def test_cells(self):
        row_key = b'cell-test'
        col = b'cf1:col1'

        await self.table.put(row_key, {col: b'old'}, timestamp=1234)
        await self.table.put(row_key, {col: b'new'})

        with self.assertRaises(TypeError):
            await self.table.cells(row_key, col, versions='invalid')  # noqa

        with self.assertRaises(TypeError):
            await self.table.cells(
                row_key, col,
                versions=3,
                timestamp='invalid',  # noqa
            )

        with self.assertRaises(ValueError):
            await self.table.cells(row_key, col, versions=0)

        results = await self.table.cells(row_key, col, versions=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(b'new', results[0])

        results = await self.table.cells(row_key, col)
        self.assertEqual(len(results), 2)
        self.assertEqual(b'new', results[0])
        self.assertEqual(b'old', results[1])

        results = await self.table.cells(
            row_key, col,
            timestamp=2345,
            include_timestamp=True,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(b'old', results[0][0])
        self.assertEqual(1234, results[0][1])

    async def test_scan(self):
        with self.assertRaises(TypeError):
            await self._scan_list(row_prefix='foobar', row_start='xyz')

        if self.connection.compat == '0.90':
            with self.assertRaises(NotImplementedError):
                await self._scan_list(filter='foo')

        with self.assertRaises(ValueError):
            await self._scan_list(limit=0)

        async with self.table.batch() as b:
            for i in range(2000):
                await b.put(f'row-scan-a{i:05}'.encode('ascii'),
                            {b'cf1:col1': b'v1',
                             b'cf1:col2': b'v2',
                             b'cf2:col1': b'v1',
                             b'cf2:col2': b'v2'})
                await b.put(f'row-scan-b{i:05}'.encode('ascii'),
                            {b'cf1:col1': b'v1', b'cf1:col2': b'v2'})

        scanner = self.table.scan(
            row_start=b'row-scan-a00012',
            row_stop=b'row-scan-a00022',
        )
        self.assertEqual(10, await self._scan_len(scanner))

        scanner = self.table.scan(row_start=b'xyz')
        self.assertEqual(0, await self._scan_len(scanner))

        scanner = self.table.scan(row_start=b'xyz', row_stop=b'zyx')
        self.assertEqual(0, await self._scan_len(scanner))

        rows = await self._scan_list(
            row_start=b'row-scan-',
            row_stop=b'row-scan-a999',
            columns=[b'cf1:col1', b'cf2:col2'],
        )
        row_key, row = rows[0]
        self.assertEqual(row_key, b'row-scan-a00000')
        self.assertDictEqual(row, {b'cf1:col1': b'v1', b'cf2:col2': b'v2'})
        self.assertEqual(2000, len(rows))

        scanner = self.table.scan(
            row_prefix=b'row-scan-a',
            batch_size=499,
            limit=1000,
        )
        self.assertEqual(1000, await self._scan_len(scanner))

        scanner = self.table.scan(
            row_prefix=b'row-scan-b',
            batch_size=1,
            limit=10,
        )
        self.assertEqual(10, await self._scan_len(scanner))

        scanner = self.table.scan(
            row_prefix=b'row-scan-b',
            batch_size=5,
            limit=10,
        )
        self.assertEqual(10, await self._scan_len(scanner))

        scanner = self.table.scan(timestamp=123)
        self.assertEqual(0, await self._scan_len(scanner))

        scanner = self.table.scan(row_prefix=b'row', timestamp=123)
        self.assertEqual(0, await self._scan_len(scanner))

        scanner = self.table.scan(batch_size=20)
        await scanner.__anext__()
        await scanner.aclose()
        with self.assertRaises(StopAsyncIteration):
            await scanner.__anext__()

    async def test_scan_sorting(self):
        if self.connection.compat < '0.96':
            return  # not supported

        input_row = {f'cf1:col-{i:03}'.encode('ascii'): b'' for i in range(100)}
        input_key = b'row-scan-sorted'
        await self.table.put(input_key, input_row)

        scan = self.table.scan(row_start=input_key, sorted_columns=True)
        key, row = await scan.__anext__()
        self.assertEqual(key, input_key)
        self.assertListEqual(sorted(input_row.items()), list(row.items()))

    async def test_scan_reverse(self):

        if self.connection.compat < '0.98':
            with self.assertRaises(NotImplementedError):
                await self._scan_list(reverse=True)
            return

        async with self.table.batch() as b:
            for i in range(2000):
                await b.put(f'row-scan-reverse-{i:04}'.encode('ascii'),
                            {b'cf1:col1': b'v1', b'cf1:col2': b'v2'})

        scan = self.table.scan(row_prefix=b'row-scan-reverse', reverse=True)
        self.assertEqual(2000, await self._scan_len(scan))

        self.assertEqual(10, await self._scan_len(limit=10, reverse=True))

        scan = self.table.scan(
            row_start=b'row-scan-reverse-1999',
            row_stop=b'row-scan-reverse-0000',
            reverse=True,
        )
        key, data = await scan.__anext__()
        self.assertEqual(b'row-scan-reverse-1999', key)

        key, data = [x async for x in scan][-1]
        self.assertEqual(b'row-scan-reverse-0001', key)

    async def test_scan_filter_and_batch_size(self):
        # See issue #54 and #56
        filt = b"SingleColumnValueFilter ('cf1', 'qual1', =, 'binary:val1')"
        async for k, v in self.table.scan(filter=filt):
            print(v)

    async def test_delete(self):
        row_key = b'row-test-delete'
        data = {b'cf1:col1': b'v1',
                b'cf1:col2': b'v2',
                b'cf1:col3': b'v3'}
        await self.table.put(row_key, {b'cf1:col2': b'v2old'}, timestamp=1234)
        await self.table.put(row_key, data)

        await self.table.delete(row_key, [b'cf1:col2'], timestamp=2345)
        cells = await self.table.cells(row_key, b'cf1:col2', versions=2)
        self.assertEqual(1, len(cells))
        self.assertDictEqual(data, await self.table.row(row_key))

        await self.table.delete(row_key, [b'cf1:col1'])
        res = await self.table.row(row_key)
        self.assertNotIn(b'cf1:col1', res)
        self.assertIn(b'cf1:col2', res)
        self.assertIn(b'cf1:col3', res)

        await self.table.delete(row_key, timestamp=12345)
        res = await self.table.row(row_key)
        self.assertIn(b'cf1:col2', res)
        self.assertIn(b'cf1:col3', res)

        await self.table.delete(row_key)
        self.assertDictEqual({}, await self.table.row(row_key))

    async def test_connection_pool_construction(self):
        with self.assertRaises(TypeError):
            ConnectionPool(size='abc')  # noqa

        with self.assertRaises(ValueError):
            ConnectionPool(size=0)

    def test_connection_pool(self):

        from thriftpy2.thrift import TException

        async def _run():
            name = threading.current_thread().name
            print("Thread %s starting" % name)

            async def inner_function():
                # Nested connection requests must return the same connection
                async with pool.connection() as another_connection:
                    assert connection is another_connection

                    # Fake an exception once in a while
                    if random.random() < .25:
                        print("Introducing random failure")
                        connection.transport.close()
                        raise TException("Fake transport exception")

            for i in range(50):
                async with pool.connection() as connection:
                    await connection.tables()

                    try:
                        await inner_function()
                    except TException:
                        # This error should have been picked up by the
                        # connection pool, and the connection should have
                        # been replaced by a fresh one
                        pass

                    await connection.tables()

            print("Thread %s done" % name)

        def run():
            loop = aio.new_event_loop()
            loop.run_until_complete(_run())
            loop.close()

        n_threads = 10
        pool = ConnectionPool(size=3, **connection_kwargs)
        threads = [threading.Thread(target=run) for _ in range(n_threads)]

        for t in threads:
            t.start()

        while threads:
            for t in threads:
                t.join(timeout=.1)

            # filter out finished threads
            threads = [t for t in threads if t.is_alive()]
            print(f"{len(threads)} threads still alive")

    async def test_pool_exhaustion(self):
        pool = ConnectionPool(size=1, **connection_kwargs)

        async def _run():
            with self.assertRaises(NoConnectionsAvailable):
                async with pool.connection(timeout=.1) as connection:
                    connection.tables()

        def run():
            loop = aio.new_event_loop()
            loop.run_until_complete(_run())
            loop.close()

        async with pool.connection():
            # At this point the only connection is assigned to this thread,
            # so another thread cannot obtain a connection at this point.

            t = threading.Thread(target=run)
            t.start()
            t.join()


if __name__ == '__main__':
    import logging
    import sys

    # Dump stacktraces using 'kill -USR1', useful for debugging hanging
    # programs and multi threading issues.
    try:
        import faulthandler  # noqa
    except ImportError:
        pass
    else:
        import signal
        faulthandler.register(signal.SIGUSR1)

    logging.basicConfig(level=logging.DEBUG)

    method_name = f'test_{sys.argv[1]}'
    method = globals()[method_name]
    method()
