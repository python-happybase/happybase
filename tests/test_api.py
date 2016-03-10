"""
HappyBase tests.
"""

import collections
import os
import random
import threading

from nose.tools import (
    assert_dict_equal,
    assert_equal,
    assert_false,
    assert_in,
    assert_is_instance,
    assert_is_not_none,
    assert_list_equal,
    assert_not_in,
    assert_raises,
    assert_true,
)

from happybase import Connection, ConnectionPool, NoConnectionsAvailable

HAPPYBASE_HOST = os.environ.get('HAPPYBASE_HOST')
HAPPYBASE_PORT = os.environ.get('HAPPYBASE_PORT')
HAPPYBASE_COMPAT = os.environ.get('HAPPYBASE_COMPAT', '0.96')
HAPPYBASE_TRANSPORT = os.environ.get('HAPPYBASE_TRANSPORT', 'buffered')
KEEP_TABLE = ('HAPPYBASE_NO_CLEANUP' in os.environ)

TABLE_PREFIX = 'happybase_tests_tmp'
TEST_TABLE_NAME = 'test1'

connection_kwargs = dict(
    host=HAPPYBASE_HOST,
    port=HAPPYBASE_PORT,
    table_prefix=TABLE_PREFIX,
    compat=HAPPYBASE_COMPAT,
    transport=HAPPYBASE_TRANSPORT,
)


# Yuck, globals
connection = table = None


def maybe_delete_table():
    if KEEP_TABLE:
        return

    if TEST_TABLE_NAME in connection.tables():
        print "Test table already exists; removing it..."
        connection.delete_table(TEST_TABLE_NAME, disable=True)


def setup_module():
    global connection, table
    connection = Connection(**connection_kwargs)

    assert_is_not_none(connection)

    maybe_delete_table()
    cfs = {
        'cf1': {},
        'cf2': None,
        'cf3': {'max_versions': 1},
    }
    connection.create_table(TEST_TABLE_NAME, families=cfs)

    table = connection.table(TEST_TABLE_NAME)
    assert_is_not_none(table)


def teardown_module():
    if not KEEP_TABLE:
        connection.delete_table(TEST_TABLE_NAME, disable=True)
    connection.close()


def test_connection_compat():
    with assert_raises(ValueError):
        Connection(compat='0.1.invalid.version')


def test_timeout_arg():
    Connection(
        timeout=5000,
        autoconnect=False)


def test_enabling():
    assert_true(connection.is_table_enabled(TEST_TABLE_NAME))
    connection.disable_table(TEST_TABLE_NAME)
    assert_false(connection.is_table_enabled(TEST_TABLE_NAME))
    connection.enable_table(TEST_TABLE_NAME)
    assert_true(connection.is_table_enabled(TEST_TABLE_NAME))


def test_compaction():
    connection.compact_table(TEST_TABLE_NAME)
    connection.compact_table(TEST_TABLE_NAME, major=True)


def test_prefix():
    assert_equal(TABLE_PREFIX + '_', connection._table_name(''))
    assert_equal(TABLE_PREFIX + '_foo', connection._table_name('foo'))

    assert_equal(connection.table('foobar').name, TABLE_PREFIX + '_foobar')
    assert_equal(connection.table('foobar', use_prefix=False).name, 'foobar')

    c = Connection(autoconnect=False)
    assert_equal('foo', c._table_name('foo'))

    with assert_raises(TypeError):
        Connection(autoconnect=False, table_prefix=123)

    with assert_raises(TypeError):
        Connection(autoconnect=False, table_prefix_separator=2.1)


def test_stringify():
    str(connection)
    repr(connection)
    str(table)
    repr(table)


def test_table_listing():
    names = connection.tables()
    assert_is_instance(names, list)
    assert_in(TEST_TABLE_NAME, names)


def test_table_regions():
    regions = table.regions()
    assert_is_instance(regions, list)


def test_invalid_table_create():
    with assert_raises(ValueError):
        connection.create_table('sometable', families={})
    with assert_raises(TypeError):
        connection.create_table('sometable', families=0)
    with assert_raises(TypeError):
        connection.create_table('sometable', families=[])


def test_families():
    families = table.families()
    for name, fdesc in families.iteritems():
        assert_is_instance(name, basestring)
        assert_is_instance(fdesc, dict)
        assert_in('name', fdesc)
        assert_in('max_versions', fdesc)


def test_put():
    table.put('r1', {'cf1:c1': 'v1', 'cf1:c2': 'v2', 'cf2:c3': 'v3'})
    table.put('r1', {'cf1:c4': 'v2'}, timestamp=2345678)
    table.put('r1', {'cf1:c4': 'v2'}, timestamp=1369168852994L)


def test_atomic_counters():
    row = 'row-with-counter'
    column = 'cf1:counter'

    assert_equal(0, table.counter_get(row, column))

    assert_equal(10, table.counter_inc(row, column, 10))
    assert_equal(10, table.counter_get(row, column))

    table.counter_set(row, column, 0)
    assert_equal(1, table.counter_inc(row, column))
    assert_equal(4, table.counter_inc(row, column, 3))
    assert_equal(4, table.counter_get(row, column))

    table.counter_set(row, column, 3)
    assert_equal(3, table.counter_get(row, column))
    assert_equal(8, table.counter_inc(row, column, 5))
    assert_equal(6, table.counter_inc(row, column, -2))
    assert_equal(5, table.counter_dec(row, column))
    assert_equal(3, table.counter_dec(row, column, 2))
    assert_equal(10, table.counter_dec(row, column, -7))


def test_batch():
    with assert_raises(TypeError):
        table.batch(timestamp='invalid')

    b = table.batch()
    b.put('row1', {'cf1:col1': 'value1',
                   'cf1:col2': 'value2'})
    b.put('row2', {'cf1:col1': 'value1',
                   'cf1:col2': 'value2',
                   'cf1:col3': 'value3'})
    b.delete('row1', ['cf1:col4'])
    b.delete('another-row')
    b.send()

    b = table.batch(timestamp=1234567)
    b.put('row1', {'cf1:col5': 'value5'})
    b.send()

    with assert_raises(ValueError):
        b = table.batch(batch_size=0)

    with assert_raises(TypeError):
        b = table.batch(transaction=True, batch_size=10)


def test_batch_context_managers():
    with table.batch() as b:
        b.put('row4', {'cf1:col3': 'value3'})
        b.put('row5', {'cf1:col4': 'value4'})
        b.put('row', {'cf1:col1': 'value1'})
        b.delete('row', ['cf1:col4'])
        b.put('row', {'cf1:col2': 'value2'})

    with table.batch(timestamp=87654321) as b:
        b.put('row', {'cf1:c3': 'somevalue',
                      'cf1:c5': 'anothervalue'})
        b.delete('row', ['cf1:c3'])

    with assert_raises(ValueError):
        with table.batch(transaction=True) as b:
            b.put('fooz', {'cf1:bar': 'baz'})
            raise ValueError
    assert_dict_equal({}, table.row('fooz', ['cf1:bar']))

    with assert_raises(ValueError):
        with table.batch(transaction=False) as b:
            b.put('fooz', {'cf1:bar': 'baz'})
            raise ValueError
    assert_dict_equal({'cf1:bar': 'baz'}, table.row('fooz', ['cf1:bar']))

    with table.batch(batch_size=5) as b:
        for i in xrange(10):
            b.put('row-batch1-%03d' % i, {'cf1:': str(i)})

    with table.batch(batch_size=20) as b:
        for i in xrange(95):
            b.put('row-batch2-%03d' % i, {'cf1:': str(i)})
    assert_equal(95, len(list(table.scan(row_prefix='row-batch2-'))))

    with table.batch(batch_size=20) as b:
        for i in xrange(95):
            b.delete('row-batch2-%03d' % i)
    assert_equal(0, len(list(table.scan(row_prefix='row-batch2-'))))


def test_row():
    row = table.row
    put = table.put
    row_key = 'row-test'

    with assert_raises(TypeError):
        row(row_key, 123)

    with assert_raises(TypeError):
        row(row_key, timestamp='invalid')

    put(row_key, {'cf1:col1': 'v1old'}, timestamp=1234)
    put(row_key, {'cf1:col1': 'v1new'}, timestamp=3456)
    put(row_key, {'cf1:col2': 'v2',
                  'cf2:col1': 'v3'})
    put(row_key, {'cf2:col2': 'v4'}, timestamp=1234)

    exp = {'cf1:col1': 'v1new',
           'cf1:col2': 'v2',
           'cf2:col1': 'v3',
           'cf2:col2': 'v4'}
    assert_dict_equal(exp, row(row_key))

    exp = {'cf1:col1': 'v1new',
           'cf1:col2': 'v2'}
    assert_dict_equal(exp, row(row_key, ['cf1']))

    exp = {'cf1:col1': 'v1new',
           'cf2:col2': 'v4'}
    assert_dict_equal(exp, row(row_key, ['cf1:col1', 'cf2:col2']))

    exp = {'cf1:col1': 'v1old',
           'cf2:col2': 'v4'}
    assert_dict_equal(exp, row(row_key, timestamp=2345))

    assert_dict_equal({}, row(row_key, timestamp=123))

    res = row(row_key, include_timestamp=True)
    assert_equal(len(res), 4)
    assert_equal('v1new', res['cf1:col1'][0])
    assert_is_instance(res['cf1:col1'][1], int)


def test_rows():
    row_keys = ['rows-row1', 'rows-row2', 'rows-row3']
    data_old = {'cf1:col1': 'v1old', 'cf1:col2': 'v2old'}
    data_new = {'cf1:col1': 'v1new', 'cf1:col2': 'v2new'}

    with assert_raises(TypeError):
        table.rows(row_keys, object())

    with assert_raises(TypeError):
        table.rows(row_keys, timestamp='invalid')

    for row_key in row_keys:
        table.put(row_key, data_old, timestamp=4000)

    for row_key in row_keys:
        table.put(row_key, data_new)

    assert_dict_equal({}, table.rows([]))

    rows = dict(table.rows(row_keys))
    for row_key in row_keys:
        assert_in(row_key, rows)
        assert_dict_equal(data_new, rows[row_key])

    rows = dict(table.rows(row_keys, timestamp=5000))
    for row_key in row_keys:
        assert_in(row_key, rows)
        assert_dict_equal(data_old, rows[row_key])


def test_cells():
    row_key = 'cell-test'
    col = 'cf1:col1'

    table.put(row_key, {col: 'old'}, timestamp=1234)
    table.put(row_key, {col: 'new'})

    with assert_raises(TypeError):
        table.cells(row_key, col, versions='invalid')

    with assert_raises(TypeError):
        table.cells(row_key, col, versions=3, timestamp='invalid')

    with assert_raises(ValueError):
        table.cells(row_key, col, versions=0)

    results = table.cells(row_key, col, versions=1)
    assert_equal(len(results), 1)
    assert_equal('new', results[0])

    results = table.cells(row_key, col)
    assert_equal(len(results), 2)
    assert_equal('new', results[0])
    assert_equal('old', results[1])

    results = table.cells(row_key, col, timestamp=2345, include_timestamp=True)
    assert_equal(len(results), 1)
    assert_equal('old', results[0][0])
    assert_equal(1234, results[0][1])


def test_scan():
    with assert_raises(TypeError):
        list(table.scan(row_prefix='foobar', row_start='xyz'))

    with assert_raises(ValueError):
        list(table.scan(batch_size=None))

    if connection.compat == '0.90':
        with assert_raises(NotImplementedError):
            list(table.scan(filter='foo'))

    with assert_raises(ValueError):
        list(table.scan(limit=0))

    with assert_raises(TypeError):
        list(table.scan(row_start='foobar', row_prefix='foo'))

    with table.batch() as b:
        for i in range(2000):
            b.put('row-scan-a%05d' % i,
                  {'cf1:col1': 'v1',
                   'cf1:col2': 'v2',
                   'cf2:col1': 'v1',
                   'cf2:col2': 'v2'})
            b.put('row-scan-b%05d' % i,
                  {'cf1:col1': 'v1',
                   'cf1:col2': 'v2'})

    def calc_len(scanner):
        d = collections.deque(maxlen=1)
        d.extend(enumerate(scanner, 1))
        if d:
            return d[0][0]
        return 0

    scanner = table.scan(row_start='row-scan-a00012',
                         row_stop='row-scan-a00022')
    assert_equal(10, calc_len(scanner))

    scanner = table.scan(row_start='xyz')
    assert_equal(0, calc_len(scanner))

    scanner = table.scan(row_start='xyz', row_stop='zyx')
    assert_equal(0, calc_len(scanner))

    scanner = table.scan(row_start='row-scan-', row_stop='row-scan-a999',
                         columns=['cf1:col1', 'cf2:col2'])
    row_key, row = next(scanner)
    assert_equal(row_key, 'row-scan-a00000')
    assert_dict_equal(row, {'cf1:col1': 'v1',
                            'cf2:col2': 'v2'})
    assert_equal(2000 - 1, calc_len(scanner))

    scanner = table.scan(row_prefix='row-scan-a', batch_size=499, limit=1000)
    assert_equal(1000, calc_len(scanner))

    scanner = table.scan(row_prefix='row-scan-b', batch_size=1, limit=10)
    assert_equal(10, calc_len(scanner))

    scanner = table.scan(row_prefix='row-scan-b', batch_size=5, limit=10)
    assert_equal(10, calc_len(scanner))

    scanner = table.scan(timestamp=123)
    assert_equal(0, calc_len(scanner))

    scanner = table.scan(row_prefix='row', timestamp=123)
    assert_equal(0, calc_len(scanner))

    scanner = table.scan(batch_size=20)
    next(scanner)
    next(scanner)
    scanner.close()
    with assert_raises(StopIteration):
        next(scanner)


def test_scan_sorting():
    if connection.compat < '0.96':
        return  # not supported

    input_row = {}
    for i in xrange(100):
        input_row['cf1:col-%03d' % i] = ''
    input_key = 'row-scan-sorted'
    table.put(input_key, input_row)

    scan = table.scan(row_start=input_key, sorted_columns=True)
    key, row = next(scan)
    assert_equal(key, input_key)
    assert_list_equal(
        sorted(input_row.items()),
        row.items())


def test_scan_filter_and_batch_size():
    # See issue #54 and #56
    filter = "SingleColumnValueFilter ('cf1', 'qual1', =, 'binary:val1')"
    for k, v in table.scan(filter=filter):
        print v


def test_delete():
    row_key = 'row-test-delete'
    data = {'cf1:col1': 'v1',
            'cf1:col2': 'v2',
            'cf1:col3': 'v3'}
    table.put(row_key, {'cf1:col2': 'v2old'}, timestamp=1234)
    table.put(row_key, data)

    table.delete(row_key, ['cf1:col2'], timestamp=2345)
    assert_equal(1, len(table.cells(row_key, 'cf1:col2', versions=2)))
    assert_dict_equal(data, table.row(row_key))

    table.delete(row_key, ['cf1:col1'])
    res = table.row(row_key)
    assert_not_in('cf1:col1', res)
    assert_in('cf1:col2', res)
    assert_in('cf1:col3', res)

    table.delete(row_key, timestamp=12345)
    res = table.row(row_key)
    assert_in('cf1:col2', res)
    assert_in('cf1:col3', res)

    table.delete(row_key)
    assert_dict_equal({}, table.row(row_key))


def test_connection_pool_construction():
    with assert_raises(TypeError):
        ConnectionPool(size='abc')

    with assert_raises(ValueError):
        ConnectionPool(size=0)


def test_connection_pool():

    from thrift.transport.TTransport import TTransportException

    def run():
        name = threading.current_thread().name
        print "Thread %s starting" % name

        def inner_function():
            # Nested connection requests must return the same connection
            with pool.connection() as another_connection:
                assert connection is another_connection

                # Fake an exception once in a while
                if random.random() < .25:
                    print "Introducing random failure"
                    connection.transport.close()
                    raise TTransportException("Fake transport exception")

        for i in xrange(50):
            with pool.connection() as connection:
                connection.tables()

                try:
                    inner_function()
                except TTransportException:
                    # This error should have been picked up by the
                    # connection pool, and the connection should have
                    # been replaced by a fresh one
                    pass

                connection.tables()

        print "Thread %s done" % name

    N_THREADS = 10

    pool = ConnectionPool(size=3, **connection_kwargs)
    threads = [threading.Thread(target=run) for i in xrange(N_THREADS)]

    for t in threads:
        t.start()

    while threads:
        for t in threads:
            t.join(timeout=.1)

        # filter out finished threads
        threads = [t for t in threads if t.is_alive()]
        print "%d threads still alive" % len(threads)


def test_pool_exhaustion():
    pool = ConnectionPool(size=1, **connection_kwargs)

    def run():
        with assert_raises(NoConnectionsAvailable):
            with pool.connection(timeout=.1) as connection:
                connection.tables()

    with pool.connection():
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
        import faulthandler
    except ImportError:
        pass
    else:
        import signal
        faulthandler.register(signal.SIGUSR1)

    logging.basicConfig(level=logging.DEBUG)

    method_name = 'test_%s' % sys.argv[1]
    method = globals()[method_name]
    method()
