#!/usr/bin/env python

"""
Relies on the Mock stuff from http://www.voidspace.org.uk/python/mock/
And unittest2 (which is pretty standard these days, seems to me)
"""

import copy
import optparse
from logging import RootLogger, getLogger
from mock import Mock, MagicMock, sentinel
from patched_unittest2 import *

import pg_rollingwindow
import psycopg2

class TestPgConnection(PatchedTestCase): pass
@TestPgConnection.patch('pg_rollingwindow.connect', spec=psycopg2.connect)
@TestPgConnection.patch('pg_rollingwindow.getLogger', spec=getLogger)
@TestPgConnection.patch('pg_rollingwindow.open', create=True)
class TestPgConnection(PatchedTestCase):

    def postSetUpPreRun(self):
        self.mock_getLogger.return_value = Mock(spec=RootLogger)
        self.mock_connect.return_value = Mock(spec=psycopg2.extensions.connection)
        self.mock_open.return_value = MagicMock(spec=file)

    def runner(self, connect_parameters):
        options = optparse.Values()
        for k,v in connect_parameters.iteritems():
            options.ensure_value(k, v)
        self.target = pg_rollingwindow.PgConnection(options)
        c = self.target.connection

    def verify(self, connect_parameters):
        legal_arguments = ('database', 'user', 'password', 'host', 'port', 'sslmode' )
        expected_arguments = dict((k,v) for k,v in connect_parameters.iteritems() if k in legal_arguments)
        self.assertEqual(1, self.mock_connect.call_count)
        self.assertDictEqual(expected_arguments, self.mock_connect.call_args[1])

    def test_no_parameters(self):
        self.runner({})
        self.verify({})

    def test_all_legal_parameters(self):
        params = {'database': 'a', 'user': 'b', 'password': 'c', 'host': 'd', 'sslmode': True}
        self.runner(params)
        self.verify(params)

    def test_illegal_parameter(self):
        params = {'fake': 'a'}
        self.runner(params)
        self.verify(params)

class TestRollingTable(PatchedTestCase):pass
@TestRollingTable.patch('pg_rollingwindow.getLogger', spec=getLogger)
class TestRollingTable(PatchedTestCase):

    fetch_queue = []

    @property
    def standard_fetch_results(self):
        return copy.deepcopy(
                [              # becomes a list, but declares as a tuple so it will get copied
                    (                                   # SELECT ...
                        (9872435,           # relid
                         'fake_attname',    # partitioning attribute name
                         10,                # step
                         'fake_non_empty_partitions_to_keep',   # non_empty_partitions_to_keep
                         'fake_reserve_partitions_to_keep',     # reserve_partitions_to_keep
                         'fake_partitioned_on', # partitioned_on
                         'fake_rolled_on',  # rolled on
                         'fake_rows',       # parent_estimated_rows
                         'fake_bytes',      # parent_total_relation_size_in_bytes
                         8423,              # data_lag_window
                         30),               # last_partition_dumped
                    ),                                  # SELECT freeze columns
                    (   ('f1',),
                        ('f2',)
                    ),
                ]
            )

    def mock_fetchone(self):
        return self.fetch_queue.pop(0) if len(self.fetch_queue) > 0 else None
    def mock_fetchall(self):
        return self.fetch_queue.pop(0) if len(self.fetch_queue) > 0 else ()

    def postSetUpPreRun(self):
        self.fetch_queue = []
        self.mock_getLogger.return_value = Mock(spec=RootLogger)
        self.mock_PgConnection = Mock(spec=pg_rollingwindow.PgConnection)
        self.mock_connection = Mock(spec=psycopg2.extensions.connection)
        self.mock_PgConnection.connection = self.mock_connection
        self.mock_cursor = Mock(spec=psycopg2.extensions.cursor)
        self.mock_connection.cursor = self.mock_cursor
        self.mock_cursor.return_value.fetchone.side_effect = self.mock_fetchone
        self.mock_cursor.return_value.fetchall.side_effect = self.mock_fetchall
        self.target = pg_rollingwindow.RollingTable(self.mock_PgConnection, 'fake_schema', 'fake_table')

    def runner(self):
        pass

    def verify_standard_fetch(self, is_managed=True, n=0):
        method_calls = self.mock_cursor.return_value.method_calls
        self.assertEqual('execute', method_calls[n][0])     # SET search_path TO ...
        self.assertEqual(('SET search_path TO %(schema)s', {'schema': 'fake_schema'}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('execute', method_calls[n][0])     # is_managed
        self.assertEqual(("""
SELECT m.relid, m.attname, m.step,
    m.non_empty_partitions_to_keep,
    m.reserve_partitions_to_keep,
    m.partitioned_on, m.rolled_on,
    floor(c.reltuples) AS reltuples,
    pg_total_relation_size(c.oid) AS total_relation_size,
    m.data_lag_window,
    m.last_partition_dumped
FROM rolling_window.maintained_table m
INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
WHERE c.relname = %(table)s
  AND n.nspname = %(schema)s
""", {'schema': 'fake_schema', 'table': 'fake_table'}),
                         method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        if not is_managed:
            return n
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT column_name FROM rolling_window.columns_to_freeze WHERE relid = %s', (9872435,)),
                         method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        return n

    def test_managed(self):
        self.fetch_queue = self.standard_fetch_results
        self.assertEqual(True, self.target.is_managed)
        self.verify_standard_fetch()

    def test_unmanaged(self):
        self.assertEqual(False, self.target.is_managed)
        self.verify_standard_fetch(is_managed=False)

    def test_add_managed(self):
        self.fetch_queue = self.standard_fetch_results
        self.assertRaises(pg_rollingwindow.UsageError,
                          self.target.add,
                          'fake_attname','fake_step',
                          'fake_non_empty_partitions_to_keep', 'fake_reserve_partitions_to_keep', 'fake_lag',
                          ['f1', 'f2'])

    def test_add(self):
        self.maxDiff = None

        expected_insert = """
INSERT INTO rolling_window.maintained_table (
    relid,
    attname,
    step,
    data_lag_window,
    non_empty_partitions_to_keep,
    reserve_partitions_to_keep)
SELECT c.oid AS relid,
    %(attname)s AS attname,
    %(step)s AS step,
    %(data_lag_window)s AS data_lag_window,
    %(non_empty_partitions_to_keep)s AS non_empty_partitions_to_keep,
    %(reserve_partitions_to_keep)s AS reserve_partitions_to_keep
FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
WHERE c.relname = %(table)s
  AND n.nspname = %(schema)s
RETURNING relid,
          last_partition_dumped
"""
        expected_insert_parameters = {'attname': 'fake_attname', 'schema': 'fake_schema',
            'non_empty_partitions_to_keep': 'fake_non_empty_partitions_to_keep',
            'reserve_partitions_to_keep': 'fake_reserve_partitions_to_keep', 'step': 'fake_step',
            'table': 'fake_table', 'data_lag_window': 'fake_lag'}

        freeze_insert = 'INSERT INTO rolling_window.columns_to_freeze (relid, column_name) VALUES (%s, %s)'
        limbo_select = 'SELECT rolling_window.add_limbo_partition(%(schema)s, %(table)s)'
        limbo_params = {'table': 'fake_table', 'schema': 'fake_schema'}

        self.fetch_queue = [
                (),     # fetch returns 0 rows
                (9872435, -1)      # INSERT ... RETURNING
            ]

        self.target.add('fake_attname','fake_step',
                'fake_non_empty_partitions_to_keep', 'fake_reserve_partitions_to_keep', 'fake_lag',
                ['f1', 'f2']
            )

        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch(is_managed=False)
        self.assertEqual('execute', method_calls[n][0])     # add -> insert
        self.assertEqual((expected_insert, expected_insert_parameters), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])    # get _relid
        n += 1
        self.assertEqual('executemany', method_calls[n][0]) # add -> insert frozen tables
        self.assertEqual(2, len(method_calls[n][1]))
        self.assertEqual(freeze_insert, method_calls[n][1][0])
#        self.assertEqual(None, method_calls[n][1][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('execute', method_calls[n][0])      # all limbo partition
        self.assertEqual((limbo_select, limbo_params), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))

    def test_roll_unmanaged(self):
        self.assertRaises(pg_rollingwindow.UsageError, self.target.roll)

    def test_roll(self):
        self.fetch_queue = self.standard_fetch_results + [

            (('created_10', ),('created_20', )),        # partitions from add_partitions_for_data_range
            (123, ), # partition1 rows created
            (234, ), # partition2 rows created
            (35, 36, 10), # min_max_in_parent_only
            ('created_30', ), # child_name
            (345, ), # rows moved
            (('reserve_40', ), ('reserve_50', )), # extend_table_reserve_partitions
            (('trimmed_60', 321, 432), ('trimmed_70', 543, 654)), # trim_expired_table_partitions
            ('1973-07-25 23:59:59 +00')
        ]

        self.target.roll()

        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch()
        self.assertEqual('execute', method_calls[n][0])     # add_partitions_for_date_range
        self.assertEqual(('SELECT a.p FROM rolling_window.add_partitions_for_data_range(%(schema)s, %(table)s, %(min_value)s, %(max_value)s) AS a(p)',
                          {'schema': 'fake_schema', 'table': 'fake_table', 'min_value': None, 'max_value': None})
                         , method_calls[n][1])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        n += 1
        self.assertEqual('execute', method_calls[n][0])     # move_data for partition1
        self.assertEqual(('SELECT rolling_window.move_data_to_partition(%(schema)s, %(table)s, %(lower_bound)s, %(clone_indexes)s, %(to_limbo)s)',
                         {'table': 'fake_table', 'clone_indexes': True, 'schema': 'fake_schema', 'lower_bound': 10, 'to_limbo': False}),
                         method_calls[n][1])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        n += 1
        self.assertEqual('execute', method_calls[n][0])     # move_data for partition2
        self.assertEqual(('SELECT rolling_window.move_data_to_partition(%(schema)s, %(table)s, %(lower_bound)s, %(clone_indexes)s, %(to_limbo)s)',
                          {'table': 'fake_table', 'clone_indexes': True, 'schema': 'fake_schema', 'lower_bound': 20, 'to_limbo': False}),
                         method_calls[n][1])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        n += 1
        self.assertEqual('execute', method_calls[n][0])     # min_max_in_parent_only
        self.assertEqual(('SELECT min_value, max_value, step FROM rolling_window.min_max_in_parent_only(%(schema)s, %(table)s)',
                         {'schema': 'fake_schema', 'table': 'fake_table'}),
                         method_calls[n][1])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        n += 1
        self.assertEqual('execute', method_calls[n][0])     # child_name
        self.assertEqual(('SELECT rolling_window.child_name(%(table)s, %(lower_bound)s)',
                         {'lower_bound': 30, 'table': 'fake_table'}),
                         method_calls[n][1])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        n += 1
        self.assertEqual('execute', method_calls[n][0])    # move_data_to_partition
        self.assertEqual(('SELECT rolling_window.move_data_to_partition(%(schema)s, %(table)s, %(lower_bound)s, %(clone_indexes)s, %(to_limbo)s)',
                        {'clone_indexes': False, 'lower_bound': 30, 'schema': 'fake_schema', 'table': 'fake_table', 'to_limbo': False}),
                        method_calls[n][1])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        n += 1
        self.assertEqual('execute', method_calls[n][0])    # move_data_to_partition
        self.assertEqual(('SELECT e.p FROM rolling_window.extend_table_reserve_partitions(%(schema)s, %(table)s) AS e(p)',
                        {'schema': 'fake_schema', 'table': 'fake_table'}),
                        method_calls[n][1])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        n += 1
        self.assertEqual('execute', method_calls[n][0])    # move_data_to_partition
        self.assertEqual(('SELECT partition_table_name, reltuples, total_relation_size_in_bytes FROM rolling_window.trim_expired_table_partitions(%(schema)s, %(table)s)',
                        {'schema': 'fake_schema', 'table': 'fake_table'}),
                        method_calls[n][1])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        n += 1
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('UPDATE rolling_window.maintained_table SET rolled_on = transaction_timestamp() WHERE relid = %(relid)s RETURNING transaction_timestamp()',
                         {'relid': 9872435}),
                        method_calls[n][1])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        n += 1
        self.assertEqual(n, len(method_calls))  # make sure we have covered all the calls

    def test_parititons_unmanaged(self):
        self.assertRaises(pg_rollingwindow.UsageError, self.target.partitions().next)

    def test_partitions(self):
        expected_results = [('t1', 234.23, 230948),('t2', 0, 0),('t3', 12309, 2309840)]
        self.fetch_queue = self.standard_fetch_results + [copy.deepcopy(expected_results)]
        for p in self.target.partitions():
            expected_tuple = expected_results.pop(0)
            expected_partition = pg_rollingwindow.RollingTable.Partition(*expected_tuple)
            self.assertEqual(expected_partition, p)
        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch()
        self.assertEqual('execute', method_calls[n][0])     # list_partitions
        self.assertEqual(('SELECT relname, floor(reltuples) AS reltuples, total_relation_size_in_bytes FROM rolling_window.list_partitions(%(schema)s, %(table)s) ORDER BY relname',
                       {'schema': 'fake_schema', 'table': 'fake_table'}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))

    def test_freeze_unmanaged(self):
        self.assertRaises(pg_rollingwindow.UsageError, self.target.freeze().next)

    def test_freeze(self):
        expected_results = [('p1', 'c1'), ('p1, c2'), ('p2', 'c3')]
        self.fetch_queue = self.standard_fetch_results + [copy.deepcopy(expected_results)]

        for fp in self.target.freeze():
            expected_tuple = expected_results.pop(0)
            expected_frozen_partition = pg_rollingwindow.RollingTable.FrozenPartition(expected_tuple[0], expected_tuple[1])
            self.assertEqual(expected_frozen_partition, fp)
        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch()
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT partition_table_name, new_constraint FROM rolling_window.freeze(%(schema)s, %(table)s)',
            {'schema': 'fake_schema', 'table': 'fake_table'}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))


class TestMaintainedTables(PatchedTestCase):pass
@TestMaintainedTables.patch('pg_rollingwindow.getLogger', spec=getLogger)
class TestMaintainedTables(PatchedTestCase):

    cursor_queue = []
    cursor_index = -1

    def mock_fetchone(self):
        self.cursor_index += 1
        if self.cursor_index >= len(self.cursor_queue):
            return None
        return self.cursor_queue[self.cursor_index]

    def mock_fetchall(self):
        self.cursor_index += 1
        if self.cursor_index >= len(self.cursor_queue):
            return []
        return self.cursor_queue[self.cursor_index]

    def postSetUpPreRun(self):
        self.mock_getLogger.return_value = Mock(spec=RootLogger)
        self.mock_PgConnection = Mock(spec=pg_rollingwindow.PgConnection)
        self.mock_connection = Mock(spec=psycopg2.extensions.connection)
        self.mock_PgConnection.connection = self.mock_connection
        self.mock_cursor = Mock(spec=psycopg2.extensions.cursor)
        self.mock_connection.cursor = self.mock_cursor
        self.mock_cursor.return_value.fetchone.side_effect = self.mock_fetchone
        self.mock_cursor.return_value.fetchall.side_effect = self.mock_fetchall
        self.target = pg_rollingwindow.MaintainedTables(self.mock_PgConnection)

    def test_no_tables(self):
        self.cursor_queue = []
        table_count = 0
        for t in self.target:
            table_count += 1
        self.assertEqual(0, table_count)
        method_calls = self.mock_cursor.return_value.method_calls
        self.assertEqual(2, len(method_calls))
        self.assertEqual('execute', method_calls[0][0])
        self.assertEqual('fetchall', method_calls[1][0])

    def test_one_table(self):
        self.cursor_queue = [[('ns', 't1')]]
        expected_tables=copy.deepcopy(self.cursor_queue[0])
        table_count = 0
        for t in self.target:
            self.assertEqual(expected_tables[table_count], t)
            table_count += 1
        self.assertEqual(1, table_count)

    def test_three_tables(self):
        self.cursor_queue = [[('ns', 't1'), ('ns', 't2'), ('ns', 't3') ]]
        expected_tables=copy.deepcopy(self.cursor_queue[0])
        table_count = 0
        for t in self.target:
            self.assertEqual(expected_tables[table_count], t)
            table_count += 1
        self.assertEqual(3, table_count)
