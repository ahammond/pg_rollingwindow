#!/usr/bin/env python

"""
Relies on the Mock stuff from http://www.voidspace.org.uk/python/mock/
And unittest2 (which is pretty standard these days, seems to me)
"""

import copy
import optparse
from logging import RootLogger, getLogger
import os
from mock import Mock, MagicMock, sentinel
from patched_unittest2 import *
from subprocess import Popen, call

import pg_rollingwindow
import psycopg2

class OptionBase(PatchedTestCase):
    def optionize(self, connect_parameters):
        options = optparse.Values()
        options.ensure_value('db', 'fake_db')       # mandatory for
        for k,v in connect_parameters.iteritems():
            options.ensure_value(k, v)
        return options


class TestPgConnection(OptionBase): pass
@TestPgConnection.patch('pg_rollingwindow.connect', spec=psycopg2.connect)
@TestPgConnection.patch('pg_rollingwindow.getLogger', spec=getLogger)
@TestPgConnection.patch('pg_rollingwindow.open', create=True)
class TestPgConnection(OptionBase):

    def postSetUpPreRun(self):
        self.mock_getLogger.return_value = Mock(spec=RootLogger)
        self.mock_connect.return_value = Mock(spec=psycopg2.extensions.connection)
        self.mock_open.return_value = MagicMock(spec=file)

    def runner(self, connect_parameters):
        self.target = pg_rollingwindow.PgConnection(self.optionize(connect_parameters))
        c = self.target.connection

    def verify(self, connect_parameters):
        legal_arguments = ('database', 'username', 'password', 'host', 'port', 'sslmode' )
        expected_arguments = dict(('user' if k == 'username' else k,v) for k,v in connect_parameters.iteritems() if k in legal_arguments)
        self.assertEqual(1, self.mock_connect.call_count)
        self.assertDictEqual(expected_arguments, self.mock_connect.call_args[1])

    def test_no_parameters(self):
        self.runner({})
        self.verify({})

    def test_all_legal_parameters(self):
        params = {'database': 'a', 'username': 'b', 'password': 'c', 'host': 'd', 'sslmode': True}
        self.runner(params)
        self.verify(params)

    def test_illegal_parameter(self):
        params = {'fake': 'a'}
        self.runner(params)
        self.verify(params)

class TestRollingWindow(PatchedTestCase):pass
@TestRollingWindow.patch('pg_rollingwindow.getLogger', spec=getLogger)
class TestRollingWindow(PatchedTestCase):

    fetch_queue = []

    @property
    def standard_fetch_results(self):
        return copy.deepcopy(
                [
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
                         3,                 # data_lag_window
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
        self.target = pg_rollingwindow.RollingWindow(self.mock_PgConnection, 'fake_schema', 'fake_table')

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
                          'fake_non_empty_partitions_to_keep', 'fake_reserve_partitions_to_keep', 'fake_lag')

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

        limbo_select = 'SELECT rolling_window.add_limbo_partition(%(schema)s, %(table)s)'
        limbo_params = {'table': 'fake_table', 'schema': 'fake_schema'}

        self.fetch_queue = [
                (),     # fetch returns 0 rows
                (9872435, -1)      # INSERT ... RETURNING
            ]

        self.target.add('fake_attname','fake_step',
                'fake_non_empty_partitions_to_keep', 'fake_reserve_partitions_to_keep', 'fake_lag'
            )

        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch(is_managed=False)
        self.assertEqual('execute', method_calls[n][0])     # add -> insert
        self.assertEqual((expected_insert, expected_insert_parameters), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])    # get _relid
        n += 1
        self.assertEqual('execute', method_calls[n][0])      # all limbo partition
        self.assertEqual((limbo_select, limbo_params), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))

    def test_freeze_column_no_overlap(self):
        self.fetch_queue = self.standard_fetch_results + [
            (True, ),
        ]
        result = self.target.freeze_column('fake_column')
        self.assertEqual(True, result)
        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch()
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT rolling_window.set_freeze_column(%(relid)s, %(column_name)s, %(lower_bound_overlap)s, %(prior_upper_bound_percentile)s)',
            {'relid': 9872435, 'column_name': 'fake_column', 'lower_bound_overlap': None,
             'prior_upper_bound_percentile': None}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))

    def test_freeze_column_overlap(self):
        self.fetch_queue = self.standard_fetch_results + [
            (True, ),
        ]
        result = self.target.freeze_column('fake_column', 'fake_overlap')
        self.assertEqual(True, result)
        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch()
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT rolling_window.set_freeze_column(%(relid)s, %(column_name)s, %(lower_bound_overlap)s, %(prior_upper_bound_percentile)s)',
            {'relid': 9872435, 'column_name': 'fake_column',
             'lower_bound_overlap': 'fake_overlap',
             'prior_upper_bound_percentile': None}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
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
            (), # indexes created
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
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT new_index FROM rolling_window.clone_indexes_to_partition(%(schema)s, %(table)s, %(lower_bound)s) AS citp(new_index)',
                              {'schema': 'fake_schema', 'table': 'fake_table', 'lower_bound': 30, }),
            method_calls[n][1])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
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

    def test_partitions(self):
        expected_results = [('t1', 234.23, None, False),('t2', 0, None, False),('t3', 12309, None, False)]
        self.fetch_queue = [copy.deepcopy(expected_results)]
        for p in self.target.partitions():
            expected_tuple = expected_results.pop(0)
            expected_partition = pg_rollingwindow.Partition(*expected_tuple)
            self.assertEqual(expected_partition, p)
        method_calls = self.mock_cursor.return_value.method_calls
        n = 0
        self.assertEqual('execute', method_calls[n][0])     # list_partitions
        self.assertEqual(('SELECT relname, floor(reltuples) AS reltuples, NULL AS total_relation_size_in_bytes, is_frozen FROM rolling_window.list_partitions(%(schema)s, %(table)s) ORDER BY relname',
                       {'schema': 'fake_schema', 'table': 'fake_table'}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))

    def test_partitions_descending(self):
        expected_results = [('t1', 234.23, None, True),('t2', 0, None, True),('t3', 12309, None, True)]
        self.fetch_queue = [copy.deepcopy(expected_results)]
        for p in self.target.partitions(descending=True):
            expected_tuple = expected_results.pop(0)
            expected_partition = pg_rollingwindow.Partition(*expected_tuple)
            self.assertEqual(expected_partition, p)
        method_calls = self.mock_cursor.return_value.method_calls
        n = 0
        self.assertEqual('execute', method_calls[n][0])     # list_partitions
        self.assertEqual(('SELECT relname, floor(reltuples) AS reltuples, NULL AS total_relation_size_in_bytes, is_frozen FROM rolling_window.list_partitions(%(schema)s, %(table)s) ORDER BY relname DESCENDING',
                       {'schema': 'fake_schema', 'table': 'fake_table'}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))

    def test_partitions_with_size(self):
        expected_results = [('t1', 234.23, 230948, True),('t2', 0, 0, True),('t3', 12309, 2309840, True)]
        self.fetch_queue = [copy.deepcopy(expected_results)]
        for p in self.target.partitions(with_size=True):
            expected_tuple = expected_results.pop(0)
            expected_partition = pg_rollingwindow.Partition(*expected_tuple)
            self.assertEqual(expected_partition, p)
        method_calls = self.mock_cursor.return_value.method_calls
        n = 0
        self.assertEqual('execute', method_calls[n][0])     # list_partitions
        self.assertEqual(('SELECT relname, floor(reltuples) AS reltuples, pg_total_relation_size(partition_table_oid) AS total_relation_size_in_bytes, is_frozen FROM rolling_window.list_partitions(%(schema)s, %(table)s) ORDER BY relname',
                       {'schema': 'fake_schema', 'table': 'fake_table'}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))

    def test_partitions_with_size_descending(self):
        expected_results = [('t1', 234.23, 230948, True),('t2', 0, 0, True),('t3', 12309, 2309840, True)]
        self.fetch_queue = [copy.deepcopy(expected_results)]
        for p in self.target.partitions(with_size=True, descending=True):
            expected_tuple = expected_results.pop(0)
            expected_partition = pg_rollingwindow.Partition(*expected_tuple)
            self.assertEqual(expected_partition, p)
        method_calls = self.mock_cursor.return_value.method_calls
        n = 0
        self.assertEqual('execute', method_calls[n][0])     # list_partitions
        self.assertEqual(('SELECT relname, floor(reltuples) AS reltuples, pg_total_relation_size(partition_table_oid) AS total_relation_size_in_bytes, is_frozen FROM rolling_window.list_partitions(%(schema)s, %(table)s) ORDER BY relname DESCENDING',
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
        self.fetch_queue = self.standard_fetch_results + [
                ('partition_2',),                           # highest_freezable
                [('partition_1', 1234, None, False), ('partition_2', 1234, None, False)],   # partitions
                ('4312'),                                   # moved to limbo
                [('c1',), ('c2',)],                         # freeze partition_1
                ('6345'),                                   # moved to limbo
                [('c3',)],                                  # freeze partition_2
            ]

        expected_results = [pg_rollingwindow.FrozenPartition('partition_1', 'c1'),
                            pg_rollingwindow.FrozenPartition('partition_1', 'c2'),
                            pg_rollingwindow.FrozenPartition('partition_2', 'c3')]
        actual_results = [x for x in self.target.freeze()]
        self.assertEqual(expected_results, actual_results)
        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch()
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT rolling_window.highest_freezable(%(schema)s, %(table)s)',
            {'schema': 'fake_schema', 'table': 'fake_table'}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT relname, floor(reltuples) AS reltuples, NULL AS total_relation_size_in_bytes, is_frozen FROM rolling_window.list_partitions(%(schema)s, %(table)s) ORDER BY relname',
            {'schema': 'fake_schema', 'table': 'fake_table'}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT rolling_window.move_data_below_lower_bound_overlap_to_limbo(%(schema)s, %(table)s, %(lower_bound)s) AS data_moved_to_limbo',
            {'schema': 'fake_schema', 'table': 'fake_table', 'lower_bound': 1}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT f.c AS new_constraint FROM rolling_window.freeze_partition(%(schema)s, %(table)s, %(lower_bound)s) AS f(c)',
            {'schema': 'fake_schema', 'table': 'fake_table', 'lower_bound': 1}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT rolling_window.move_data_below_lower_bound_overlap_to_limbo(%(schema)s, %(table)s, %(lower_bound)s) AS data_moved_to_limbo',
            {'schema': 'fake_schema', 'table': 'fake_table', 'lower_bound': 2}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT f.c AS new_constraint FROM rolling_window.freeze_partition(%(schema)s, %(table)s, %(lower_bound)s) AS f(c)',
            {'schema': 'fake_schema', 'table': 'fake_table', 'lower_bound': 2}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchall', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))

    def test_highest_freezable(self):
        self.fetch_queue = self.standard_fetch_results + [('p_00006', )]
        self.assertEqual('p_00006', self.target.highest_freezable)
        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch()
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(('SELECT rolling_window.highest_freezable(%(schema)s, %(table)s)',
                          {'schema': 'fake_schema', 'table': 'fake_table'}), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual('fetchone', method_calls[n][0])
        self.assertEqual((), method_calls[n][1])
        self.assertEqual({}, method_calls[n][2])
        n += 1
        self.assertEqual(n, len(method_calls))

    def test_last_partition_dumped_setter(self):
        self.fetch_queue = self.standard_fetch_results
        self.mock_cursor.return_value.rowcount = 1
        self.target.last_partition_dumped = 12345
        self.assertEqual(12345, self.target.last_partition_dumped)
        method_calls = self.mock_cursor.return_value.method_calls
        n = self.verify_standard_fetch()
        self.assertEqual('execute', method_calls[n][0])
        self.assertEqual(("""
UPDATE rolling_window.maintained_table
SET last_partition_dumped = %(last_partition_dumped)s
FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
WHERE rolling_window.maintained_table.relid = c.oid
  AND c.relname = %(table)s
  AND n.nspname = %(schema)s
""", {'schema': 'fake_schema', 'table': 'fake_table',
      'last_partition_dumped': 12345}), method_calls[n][1])
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

class TestPartitionDumper(OptionBase):pass
@TestPartitionDumper.patch('pg_rollingwindow.getLogger', spec=getLogger)
@TestPartitionDumper.patch('pg_rollingwindow.access', spec=os.access)
#@TestPartitionDumper.patch('pg_rollingwindow.environ', spec=None)  # TODO: patch_dict?  need to test PGPATH stuff.
@TestPartitionDumper.patch('pg_rollingwindow.listdir', spec=os.listdir)
@TestPartitionDumper.patch('pg_rollingwindow.rename', spec=os.rename)
@TestPartitionDumper.patch('pg_rollingwindow.exists', spec=os.path.exists)
@TestPartitionDumper.patch('pg_rollingwindow.isdir', spec=os.path.isdir)
@TestPartitionDumper.patch('pg_rollingwindow.isfile', spec=os.path.isfile)
@TestPartitionDumper.patch('pg_rollingwindow.call', spec=call)
class TestPartitionDumper(OptionBase):
    maxDiff = None

    def postSetUpPreRun(self):
        _partitions = [
                pg_rollingwindow.Partition('fake_table_0000000000000000000', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000010', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000020', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000030', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000040', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000050', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000060', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000070', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000080', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000090', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_limbo', 10, 10, False),
            ]
        _list_dir = [
                'fake_schema.fake_table.pgdump',            # the parent table
                'fake_schema.fake_table_0000000000000000000.pgdump',
                'fake_schema.fake_table_0000000000000000010.pgdump',
                'fake_schema.fake_table_0000000000000000020.pgdump',
                'fake_schema.fake_table_0000000000000000030.pgdump',
                'fake_schema.fake_table_0000000000000000040.pgdump',
            ]

        self.mock_getLogger.return_value = Mock(spec=RootLogger)
        rw = MagicMock(spec=pg_rollingwindow.RollingWindow)
        self.mock_RollingWindow = rw
        rw.table = 'fake_table'
        rw.schema = 'fake_schema'
        rw.relid = 1234     # the parent table should appear to be loaded by default
        rw.last_partition_dumped = 40
        rw.highest_freezable = 'fake_table_0000000000000000080'     #TODO: this should really be a property so we can see if it was called.
        partitions_generator = rw.partitions.return_value
        partitions_generator.__iter__.return_value = iter(_partitions)
        self.mock_access.return_value = True
        self.mock_listdir.return_value = _list_dir
        self.mock_exists.return_value = True
        self.mock_isdir.return_value = True
        self.mock_isfile.return_value = True
        self.mock_call.return_value = 0

    def runner(self, connect_parameters, ):
        self.target = pg_rollingwindow.PartitionDumper(self.optionize(connect_parameters))

    def verify(self, connect_parameters):
        pass_along_arguments = ('username', 'host', 'port')
        expected_arguments = dict((k,v) for k,v in connect_parameters.iteritems() if k in pass_along_arguments)
        # TODO: more stuff to verify that is common between calls?

    def test_missing_dump_directory_option(self):
        options = self.optionize({})
        self.assertRaises(pg_rollingwindow.UsageError,  pg_rollingwindow.PartitionDumper, options)

    def test_dump_table_dump_directory_not_exists(self):
        o = dict(dump_directory='/fake/dir')
        self.mock_exists.return_value = False
        self.runner(o)
        self.verify(o)
        self.assertRaises(pg_rollingwindow.UsageError, self.target.dump_table, self.mock_RollingWindow, 'fake_partition')

    def test_restore_file_dump_directory_not_exists(self):
        o = dict(dump_directory='/fake/dir')
        self.mock_exists.return_value = False
        self.runner(o)
        self.verify(o)
        self.assertRaises(pg_rollingwindow.UsageError, self.target.restore_file, self.mock_RollingWindow, 'fake_filename')

    def test_dump_table_dump_directory_not_isdir(self):
        o = dict(dump_directory='/fake/dir')
        self.mock_isdir.return_value = False
        self.runner(o)
        self.verify(o)
        self.assertRaises(pg_rollingwindow.UsageError, self.target.dump_table, self.mock_RollingWindow, 'fake_partition')

    def test_restore_file_dump_directory_not_isdir(self):
        o = dict(dump_directory='/fake/dir')
        self.mock_isdir.return_value = False
        self.runner(o)
        self.verify(o)
        self.assertRaises(pg_rollingwindow.UsageError, self.target.restore_file, self.mock_RollingWindow, 'fake_filename')

    def test_dump_table_dump_directory_not_access(self):
        o = dict(dump_directory='/fake/dir')
        self.mock_access.return_value = False
        self.runner(o)
        self.verify(o)
        self.assertRaises(pg_rollingwindow.UsageError, self.target.dump_table, self.mock_RollingWindow, 'fake_partition')

    def test_restore_file_dump_directory_not_access(self):
        o = dict(dump_directory='/fake/dir')
        self.mock_access.return_value = False
        self.runner(o)
        self.verify(o)
        self.assertRaises(pg_rollingwindow.UsageError, self.target.restore_file, self.mock_RollingWindow, 'fake_filename')

    def test_dump_table_go_right(self):
        o = dict(dump_directory='/fake/dir', database='fake_database')
        self.runner(o)
        self.target.dump_table(self.mock_RollingWindow, 'fake_table_0000000000000000000')
        self.verify(o)
        self.assertEqual(
            [((['pg_dump', '--format=custom', '--compress=9', '--no-password',
                '--file=/fake/dir/fake_schema.fake_table_0000000000000000000.pgdump.partial',
                '--table=fake_schema.fake_table_0000000000000000000', 'fake_database'], ),
              {})],
            self.mock_call.call_args_list)
        # check that last_partition_dumped was updated
#        self.mock_RollingWindow.call_args_list
        self.assertEqual(
            [(('/fake/dir/fake_schema.fake_table_0000000000000000000.pgdump.partial',
               '/fake/dir/fake_schema.fake_table_0000000000000000000.pgdump'),
              {})],
            self.mock_rename.call_args_list)

    def test_dump_table_weird_partition(self):
        o = dict(dump_directory='/fake/dir', database='fake_database')
        self.runner(o)
        self.target.dump_table(self.mock_RollingWindow, 'weird_partition_name')
        self.verify(o)
        self.assertEqual(
            [((['pg_dump', '--format=custom', '--compress=9', '--no-password',
                '--file=/fake/dir/fake_schema.weird_partition_name.pgdump.partial',
                '--table=fake_schema.weird_partition_name', 'fake_database'], ),
              {})],
            self.mock_call.call_args_list)
        # check that last_partition_dumped was NOT updated
#        self.mock_RollingWindow.call_args_list
        self.assertEqual(
            [(('/fake/dir/fake_schema.weird_partition_name.pgdump.partial',
               '/fake/dir/fake_schema.weird_partition_name.pgdump'),
              {})],
            self.mock_rename.call_args_list)

    def test_dump_table_missing_database(self):
        o = dict(dump_directory='/fake/dir')
        self.runner(o)
        self.target.dump_table(self.mock_RollingWindow, 'fake_table_0000010')
        self.verify(o)
        self.assertEqual(
            [((['pg_dump', '--format=custom', '--compress=9', '--no-password',
                '--file=/fake/dir/fake_schema.fake_table_0000010.pgdump.partial',
                '--table=fake_schema.fake_table_0000010'], ),
              {})],
            self.mock_call.call_args_list)
        # check that last_partition_dumped was updated
#        self.mock_RollingWindow.call_args_list
        self.assertEqual(
            [(('/fake/dir/fake_schema.fake_table_0000010.pgdump.partial',
               '/fake/dir/fake_schema.fake_table_0000010.pgdump'),
              {})],
            self.mock_rename.call_args_list)

    def test_eligible_to_dump_go_right(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.runner(o)
        actual_results = [e for e in self.target.eligible_to_dump(self.mock_RollingWindow)]
        self.verify(o)
        self.assertEqual([
            pg_rollingwindow.EligiblePartition('fake_table_0000000000000000050',False),
            pg_rollingwindow.EligiblePartition("fake_table_0000000000000000060",False),
            pg_rollingwindow.EligiblePartition("fake_table_0000000000000000070",False),
            pg_rollingwindow.EligiblePartition("fake_table_0000000000000000080",False)
        ], actual_results)
        log_calls = self.mock_getLogger.return_value.method_calls
        n = 0
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Examining %s.%s for dumping. Highest freezable partition is %s', 'fake_schema', 'fake_table', 'fake_table_0000000000000000080'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000000'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('I think I already dumped %s since %d is less than %d', 'fake_table_0000000000000000000', 0, 40), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000010'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('I think I already dumped %s since %d is less than %d', 'fake_table_0000000000000000010', 10, 40), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000020'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('I think I already dumped %s since %d is less than %d', 'fake_table_0000000000000000020', 20, 40), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000030'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('I think I already dumped %s since %d is less than %d', 'fake_table_0000000000000000030', 30, 40), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000040'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('I think I already dumped %s since %d is less than %d', 'fake_table_0000000000000000040', 40, 40), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000050'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('%s looks eligible', 'fake_table_0000000000000000050'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000060'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('%s looks eligible', 'fake_table_0000000000000000060'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000070'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('%s looks eligible', 'fake_table_0000000000000000070'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000080'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('%s looks eligible', 'fake_table_0000000000000000080'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000090'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('%s is higher than %s, so we have dumped all the freezable tables.', 'fake_table_0000000000000000090', 'fake_table_0000000000000000080'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual(n, len(log_calls))

    def test_eligible_to_dump_parent_partition(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.runner(o)
        self.mock_RollingWindow.last_partition_dumped = -1
        self.mock_RollingWindow.highest_freezable = None
        actual_results = [e for e in self.target.eligible_to_dump(self.mock_RollingWindow)]
        self.verify(o)
        self.assertEqual([pg_rollingwindow.EligiblePartition('fake_table', True)], actual_results)
        log_calls = self.mock_getLogger.return_value.method_calls
        n = 0
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Examining %s.%s for dumping. Highest freezable partition is %s', 'fake_schema', 'fake_table', None), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('info', log_calls[n][0])
        self.assertEqual(('Parent table is eligible', ), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual(n, len(log_calls))

    def test_dump_skips_strange_partition(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.runner(o)
        _partitions = [
                pg_rollingwindow.Partition('a_weird_partition', 10, 10, False),
                pg_rollingwindow.Partition('fake_table_0000000000000000000', 10, 10, True),
                pg_rollingwindow.Partition('fake_table_0000000000000000010', 10, 10, True),
        ]
        self.mock_RollingWindow.partitions.return_value.__iter__.return_value = iter(_partitions)
        self.mock_RollingWindow.last_partition_dumped = 0
        self.mock_RollingWindow.highest_freezable = 'fake_table_0000000000000000000'
        actual_results = [e for e in self.target.eligible_to_dump(self.mock_RollingWindow)]
        self.verify(o)
#        self.assertEqual([pg_rollingwindow.EligiblePartition('fake_table_0000010', True)], actual_results)
        log_calls = self.mock_getLogger.return_value.method_calls
        n = 0
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Examining %s.%s for dumping. Highest freezable partition is %s', 'fake_schema', 'fake_table', 'fake_table_0000000000000000000'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'a_weird_partition'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('warning', log_calls[n][0])
        self.assertEqual(('Skipping strange partition: %s', 'a_weird_partition'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000000'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('I think I already dumped %s since %d is less than %d', 'fake_table_0000000000000000000', 0, 0), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('Considering %s for eligibility', 'fake_table_0000000000000000010'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual('debug', log_calls[n][0])
        self.assertEqual(('%s is higher than %s, so we have dumped all the freezable tables.', 'fake_table_0000000000000000010', 'fake_table_0000000000000000000'), log_calls[n][1])
        self.assertEqual({}, log_calls[n][2])
        n += 1
        self.assertEqual(n, len(log_calls))

    def test_restore_table_go_right(self):
        o = dict(dump_directory='/fake/dir', database='fake_database')
        self.runner(o)
        self.target.restore_file(self.mock_RollingWindow, '/fake/dir/fake_schema_fake_table_0000000.pgdump')
        self.verify(o)
        self.assertEqual(
            [((['pg_restore', '--format=custom', '--jobs=4', '--no-password', '--exit-on-error',
                '--dbname=fake_database', '/fake/dir/fake_schema_fake_table_0000000.pgdump'], ),
              {})],
            self.mock_call.call_args_list)

    def test_tables_eligible_to_restore_with_parent(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.runner(o)
        actual_results = [e for e in self.target.tables_eligible_to_restore()]
        self.verify(o)
        self.assertEqual([
                pg_rollingwindow.RollingWindow('fake_db', 'fake_schema', 'fake_table'),
            ], actual_results)

    def test_tables_eligible_to_restore_no_parent(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.mock_listdir.return_value = [
                'fake_schema.fake_table_0000000000000000050.pgdump',
                'fake_schema.fake_table_0000000000000000060.pgdump',
                'fake_schema.fake_table_0000000000000000070.pgdump',
                'fake_schema.fake_table_0000000000000000080.pgdump',
                'fake_schema.fake_table_0000000000000000090.pgdump',
                'fake_schema.fake_table_0000000000000000100.pgdump',
                'fake_schema.fake_table_0000000000000000110.pgdump',
                'fake_schema.fake_table_0000000000000000120.pgdump',
                'fake_schema.fake_table_0000000000000000130.pgdump',
                'fake_schema.fake_table_0000000000000000140.pgdump',
            ]
        self.runner(o)
        actual_results = [e for e in self.target.tables_eligible_to_restore()]
        self.verify(o)
        self.assertEqual([
                pg_rollingwindow.RollingWindow('fake_db', 'fake_schema', 'fake_table'),
            ], actual_results)

    def test_tables_eligible_to_restore_two(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.mock_listdir.return_value = [
                'fake_schema.fake_table_0000000000000000050.pgdump',
                'fake_schema.other_table_0000000000000000050.pgdump',
            ]
        self.runner(o)
        actual_results = [e for e in self.target.tables_eligible_to_restore()]
        self.verify(o)
        self.assertEqual([
                pg_rollingwindow.RollingWindow('fake_db', 'fake_schema', 'fake_table'),
                pg_rollingwindow.RollingWindow('fake_db', 'fake_schema', 'other_table'),
            ], actual_results)

    def test_partitions_eligible_to_restore_already_loaded(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.runner(o)
        actual_results = [e for e in self.target.files_eligible_to_restore(self.mock_RollingWindow)]
        self.verify(o)
        self.assertEqual([], actual_results)

    def test_partitions_eligible_to_restore_parent_eligible(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.mock_RollingWindow.relid = None     # the parent table isn't loaded
        self.mock_RollingWindow.partitions.return_value.__iter__.return_value = iter([])    # and no partitions.
        self.runner(o)
        actual_results = [e for e in self.target.files_eligible_to_restore(self.mock_RollingWindow)]
        self.verify(o)
        self.assertEqual([
                'fake_dumpdir/fake_schema.fake_table.pgdump',
                'fake_dumpdir/fake_schema.fake_table_0000000000000000000.pgdump',
                'fake_dumpdir/fake_schema.fake_table_0000000000000000010.pgdump',
                'fake_dumpdir/fake_schema.fake_table_0000000000000000020.pgdump',
                'fake_dumpdir/fake_schema.fake_table_0000000000000000030.pgdump',
                'fake_dumpdir/fake_schema.fake_table_0000000000000000040.pgdump',
            ], actual_results)    # only parent eligible for load.

    def test_partitions_eligible_to_restore_five_eligible(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.mock_listdir.return_value = [
                'fake_schema.fake_table_0000000000000000050.pgdump',    # already loaded
                'fake_schema.fake_table_0000000000000000060.pgdump',
                'fake_schema.fake_table_0000000000000000070.pgdump',
                'fake_schema.fake_table_0000000000000000080.pgdump',
                'fake_schema.fake_table_0000000000000000090.pgdump',
                'fake_schema.fake_table_0000000000000000100.pgdump',    # new
                'fake_schema.fake_table_0000000000000000110.pgdump',
                'fake_schema.fake_table_0000000000000000120.pgdump',
                'fake_schema.fake_table_0000000000000000130.pgdump',
                'fake_schema.fake_table_0000000000000000140.pgdump',
            ]
        self.runner(o)
        actual_results = [e for e in self.target.files_eligible_to_restore(self.mock_RollingWindow)]
        self.verify(o)
        self.assertEqual([
             'fake_dumpdir/fake_schema.fake_table_0000000000000000100.pgdump',
             'fake_dumpdir/fake_schema.fake_table_0000000000000000110.pgdump',
             'fake_dumpdir/fake_schema.fake_table_0000000000000000120.pgdump',
             'fake_dumpdir/fake_schema.fake_table_0000000000000000130.pgdump',
             'fake_dumpdir/fake_schema.fake_table_0000000000000000140.pgdump',
            ], actual_results)

    def test_partitions_eligible_to_restore_not_isfile(self):
        o = dict(database='fake_db', host='fake_host', port='fake_port', dump_directory='fake_dumpdir', pg_path='fake_path')
        self.mock_listdir.return_value = [
                'fake_schema.fake_table_0000000000000000050.pgdump',    # already loaded
                'fake_schema.fake_table_0000000000000000060.pgdump',
                'fake_schema.fake_table_0000000000000000070.pgdump',
                'fake_schema.fake_table_0000000000000000080.pgdump',
                'fake_schema.fake_table_0000000000000000090.pgdump',
                'fake_schema.fake_table_0000000000000000100.pgdump',    # new
                'fake_schema.fake_table_0000000000000000110.pgdump',
                'fake_schema.fake_table_0000000000000000120.pgdump',
                'fake_schema.fake_table_0000000000000000130.pgdump',
                'fake_schema.fake_table_0000000000000000140.pgdump',
            ]
        self.mock_isfile.return_value = False
        self.runner(o)
        actual_results = [e for e in self.target.files_eligible_to_restore(self.mock_RollingWindow)]
        self.verify(o)
        self.assertEqual([], actual_results)


class TestSchemaInitializer(OptionBase):pass
@TestSchemaInitializer.patch('pg_rollingwindow.getLogger', spec=getLogger)
@TestSchemaInitializer.patch('pg_rollingwindow.path_split', spec=os.path.split)
@TestSchemaInitializer.patch('pg_rollingwindow.call', spec=call)
class TestSchemaInitializer(OptionBase):
    maxDiff = None

    def postSetUpPreRun(self):
        self.mock_path_split.return_value = ('fake_head', 'fake_tail')
        self.mock_call.return_value = 0

    def test_go_right(self):
        target = pg_rollingwindow.SchemaInitializer(self.optionize({}))
        target.initialize_schema()

        self.assertEqual(
            [((['psql', '--file=fake_head/pg_rollingwindow_api.sql'],), {})],
            self.mock_call.call_args_list
        )
