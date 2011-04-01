#!/usr/bin/env python
from logging import getLogger
from math import floor
from optparse import OptionParser, OptionGroup, Values
from psycopg2 import connect, IntegrityError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED

__author__ = 'Andrew Hammond <andrew.hammond@receipt.com>'
__copyright__ = 'Copyright (c) 2011 SmartReceipt'
__license__ = 'PostgreSQL License, see http://www.postgresql.org/about/license'
__vcs_id__ = '$Id$'

##########################################################################
class UsageError(Exception):
    pass

##########################################################################
class PgConnection(object):
    """Wrap psycopg2 connection to have a single persistant connection.

    This isn't a multi-treaded application. Keep it simple.
    """
    legal_arguments = ('database', 'user', 'password', 'host', 'port', 'sslmode' )

    def __init__(self, options):
        l = getLogger('PgConnection.__init__')
        l.debug('init')
        self._connection = None
        self.arguments = {}
        for k in dir(options):
            if k in self.legal_arguments:
                v = eval('options.%s' % k)
                if v is not None:
                    self.arguments[k] = v

    @property
    def connection(self):
        l = getLogger('PgConnection.connection')
        if self._connection is None:
            if len(self.arguments):
                printable_arguments = dict((k,v) for k,v in self.arguments.iteritems() if k != 'password')
                l.debug('Connecting with arguments (not including any password): %s', repr(printable_arguments))
                self._connection = connect(**self.arguments)
            else:
                l.debug('Connecting with dsn=\'\' as there are no PostgreSQL connection options.')
                self._connection = connect('')
            self._connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return self._connection

##########################################################################
class RollingTable(object):
    """Encapsulate handling of a table and it's partitions.

    """
    CREATED = 'c'
    MOVED = 'm'

    def __init__(self, db, schema, table):
        l = getLogger('RollingTable.init')
        l.debug('RollingTable: %s, %s', schema, table)
        self.db = db
        self.schema = schema
        self.table = table
        self._is_managed = None
        self._relid = None
        self._attname = None
        self._step = None
        self._non_empty_partitions_to_keep = None
        self._reserve_partitions_to_keep = None
        self._partitioned_on = None
        self._partitions_to_keep = None
        self._rolled_on = None
        self._freeze_columns = None
        self._parent_estimated_rows = None
        self._parent_total_relation_size_in_bytes = None
        self._data_lag_window = None
        self._last_partition_dumped = None

    def fetch(self):
        l = getLogger('RollingTable.fetch')
        l.debug('fetching %s.%s', self.schema, self.table)
        cursor = self.db.connection.cursor()
        cursor.execute('SET search_path TO %(schema)s', {'schema': self.schema})
        cursor.execute("""
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
""", {'schema': self.schema, 'table': self.table})
        results = cursor.fetchall()
        if len(results) < 1:
            self._is_managed = False
        else:
            self._is_managed = True
            (self._relid, self._attname, self._step,
             self._non_empty_partitions_to_keep,
             self._reserve_partitions_to_keep,
             self._partitioned_on, self._rolled_on,
             self._parent_estimated_rows,
             self._parent_total_relation_size_in_bytes,
             self._data_lag_window,
             self._last_partition_dumped
            ) = results[0]
            cursor.execute('SELECT column_name FROM rolling_window.columns_to_freeze WHERE relid = %s', (self._relid,))
            self._freeze_columns = [r[0] for r in cursor.fetchall()]

    @property
    def is_managed(self):
        if self._is_managed is None:
            self.fetch()
        return self._is_managed

    @property
    def relid(self):
        if not self.is_managed:     # implicitly calls fetch
            raise UsageError('Table %s.%s either does not exist or is not managed. It must be added before it can be fetched.', self.schema, self.table)
        return int(self._relid)

    @property
    def step(self):
        if not self.is_managed:     # implicitly calls fetch
            raise UsageError('Table %s.%s either does not exist or is not managed. It must be added before it can be fetched.', self.schema, self.table)
        return int(self._step)

    @property
    def parent_estimated_rows(self):
        if not self.is_managed:
            raise UsageError('Table %s.%s either does not exist or is not managed. It must be added before it can be fetched.', self.schema, self.table)
        return int(self._parent_estimated_rows)

    @property
    def parent_total_relation_size_in_bytes(self):
        if not self.is_managed:
            raise UsageError('Table %s.%s either does not exist or is not managed. It must be added before it can be fetched.', self.schema, self.table)
        return self._parent_total_relation_size_in_bytes

    @property
    def rolled_on(self):
        if not self.is_managed:
            raise UsageError('Table %s.%s either does not exist or is not managed. It must be added before it can be fetched.', self.schema, self.table)
        return self._rolled_on

    @property
    def last_partition_dumped(self):
        if not self.is_managed:
            raise UsageError('Table %s.%s either does not exist or is not managed. It must be added before it can be fetched.', self.schema, self.table)
        return self._last_partition_dumped

    def add(self, attname, step,
            non_empty_partitions_to_keep, reserve_partitions_to_keep,
            data_lag_window,
            freeze_columns):
        """Add a table to the list of tables under management."""
        l = getLogger('RollingWindow.add')
        l.debug('Adding %s.%s', self.schema, self.table)
        if self.is_managed:
            raise UsageError('May not add a table which is already managed.')
        self._attname = attname
        self._step = step
        self._non_empty_partitions_to_keep = non_empty_partitions_to_keep
        self._reserve_partitions_to_keep = reserve_partitions_to_keep
        self._freeze_columns = freeze_columns
        self._data_lag_window = data_lag_window
        self.db.connection.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        cursor = self.db.connection.cursor()
        # TODO: should test to make sure that attname column is non-nullable in catalog.
        cursor.execute ("""
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
""", {'schema': self.schema, 'table': self.table,
      'attname': attname, 'step': step,
      'data_lag_window': data_lag_window,
      'non_empty_partitions_to_keep': non_empty_partitions_to_keep,
      'reserve_partitions_to_keep': reserve_partitions_to_keep})
        if cursor.rowcount < 1:     # can't be more than 1 given catalog schema
            raise UsageError('No row inserted. Does %s.%s exist?' % (self.schema, self.table))
        self._relid, self._last_partition_dumped = cursor.fetchone()
        if len(freeze_columns) > 0:
            l.debug('adding freeze_columns: %s', freeze_columns)
            cursor.executemany('INSERT INTO rolling_window.columns_to_freeze (relid, column_name) VALUES (%s, %s)',
                               [(self._relid, x) for x in freeze_columns])
        cursor.execute('SELECT rolling_window.add_limbo_partition(%(schema)s, %(table)s)',
            {'schema': self.schema, 'table': self.table})
        if cursor.rowcount < 1:
            raise UsageError('Limbo table not created? Why not?!')
        self._is_managed = True
        self.db.connection.commit()
        self.db.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    class PartitionResult(object):
        def __init__(self, method, partition_name, rows_moved):
            self._method = method
            self._partition_name = partition_name
            self._rows_moved = rows_moved

        @property
        def method(self):
            return self._method

        @property
        def partition_name(self):
            return self._partition_name

        @property
        def rows_moved(self):
            return self._rows_moved

    def partition(self, clone_indexes=True):
        """Partition this table by creating partitions to cover the entire span of data in the table and then moving data into the partitions.

        Parititon tables are created covering the lowest to highest existing data.
        Data is then moved to the partitions, iterating from lowest to highest.
        Partitions have indexes cloned onto them following each move of the data (unless clone_indexes is set to false).
        """
        l = getLogger('RollingWindow.partition')
        l.debug('Partitioning %s.%s', self.schema, self.table)
        if not self.is_managed:
            raise UsageError('Can not partition a table that is not managed.')
        cursor = self.db.connection.cursor()
        cursor.execute('SELECT a.p FROM rolling_window.add_partitions_for_data_range(%(schema)s, %(table)s, %(min_value)s, %(max_value)s) AS a(p)',
                       {'schema': self.schema, 'table': self.table, 'min_value': None, 'max_value': None})
        partitions_created = [result[0] for result in cursor.fetchall()]
        for partition_created in partitions_created:
            l.debug('Created %s', partition_created)
            lower_bound = int(partition_created[partition_created.rindex('_')+1:])
            cursor.execute('SELECT rolling_window.move_data_to_partition(%(schema)s, %(table)s, %(lower_bound)s, %(clone_indexes)s, %(to_limbo)s)',
                           {'schema': self.schema, 'table': self.table, 'lower_bound': lower_bound,
                            'clone_indexes': clone_indexes, 'to_limbo': False})
            rows_moved = cursor.fetchone()[0]
            l.info('Created partition %s and moved %s rows.', partition_created, rows_moved)
            yield self.PartitionResult(self.CREATED, partition_created, rows_moved)
        # Handle any data left lurking in the parent table.
        cursor.execute('SELECT min_value, max_value, step FROM rolling_window.min_max_in_parent_only(%(schema)s, %(table)s)',
                       {'schema': self.schema, 'table': self.table})
        min_value, max_value, step = cursor.fetchone()
        if min_value is not None:
            start_value = min_value - min_value % step
            stop_value = step + max_value - max_value % step
            for lower_bound in range(start_value, stop_value, step):
                cursor.execute('SELECT rolling_window.child_name(%(table)s, %(lower_bound)s)',
                               {'table': self.table, 'lower_bound': lower_bound})
                partition = cursor.fetchone()[0]
                # since the partition already exists, we should not clone indexes as they are probably already there.
                try:
                    cursor.execute('SELECT rolling_window.move_data_to_partition(%(schema)s, %(table)s, %(lower_bound)s, %(clone_indexes)s, %(to_limbo)s)',
                                   {'schema': self.schema, 'table': self.table, 'lower_bound': lower_bound,
                                    'clone_indexes': False, 'to_limbo': False})
                    rows_moved = cursor.fetchone()[0]
                    l.info('Moved %s rows to partition %s.', partition, rows_moved)
                except IntegrityError, e:
                    l.warn('Failed to move data to partition for %s.%s %s: %s', self.schema, self.table, lower_bound, e)
                    #TODO: a more fine-grained approach around sending data to limbo would be nice. This sends entire chunks of data...
                    cursor.execute('SELECT rolling_window.move_data_to_partition(%(schema)s, %(table)s, %(lower_bound)s, %(clone_indexes)s, %(to_limbo)s)',
                        {'schema': self.schema, 'table': self.table, 'lower_bound': lower_bound,
                         'clone_indexes': False, 'to_limbo': True})
                    rows_moved = cursor.fetchone()[0]
                    l.info('Moved %s rows to %s.%s_limbo', rows_moved, self.schema, self.table)
                yield self.PartitionResult(self.MOVED, partition, rows_moved)

    def roll(self):
        """Perform standard maintenance on a maintained table.

        Move any data in the parent table to the appropriate partition, creating partitions as necessary.
        Add new reserve partitions ahead of the data window as necessary.
        Trim partitions from the data window as necessary.
        Gets details about how many reserve partitions to create and how many data partitons to retain from the rolling_window.maintained_table table.
        """
        l = getLogger('RollingWindow.roll')
        l.debug('Rolling %s.%s', self.schema, self.table)
        if not self.is_managed:
            raise UsageError('Can not partition a table that is not managed.')
        partitions_created, rows_to_created_partitions, partition_only_moved_to, rows_to_existing_partitions = [0] * 4
        for x in self.partition():
            if x.method == self.CREATED:
                partitions_created += 1
                rows_to_created_partitions += x.rows_moved
            elif x.method == self.MOVED:
                partition_only_moved_to += 1
                rows_to_existing_partitions += x.rows_moved
        l.info('Created %s new partitions moving %s rows and moved %s rows to %s existing partitions',
               partitions_created, rows_to_created_partitions, rows_to_existing_partitions, partition_only_moved_to)
        cursor = self.db.connection.cursor()
        reserve_partitions_created = 0
        cursor.execute('SELECT e.p FROM rolling_window.extend_table_reserve_partitions(%(schema)s, %(table)s) AS e(p)',
                       {'schema': self.schema, 'table': self.table})
        for result in cursor.fetchall():
            reserve_partition_created = result[0]
            l.debug('Created reserve partition %s', reserve_partition_created)
            reserve_partitions_created += 1
        l.info('Created %d reserve partitions on %s', reserve_partitions_created, self.table)
        cursor.execute('SELECT partition_table_name, reltuples, total_relation_size_in_bytes FROM rolling_window.trim_expired_table_partitions(%(schema)s, %(table)s)',
                       {'schema': self.schema, 'table': self.table})
        partitions_trimmed, total_rows_trimmed, total_bytes_reclaimed = [0] * 3
        for r in cursor.fetchall():
            partition_trimmed, rows_trimmed, bytes_reclaimed = r
            l.debug('Trimmed partition %s, removed approximately %s rows and reclaimed %s bytes',
                    partition_trimmed, rows_trimmed, bytes_reclaimed)
            partitions_trimmed += 1
            total_rows_trimmed += rows_trimmed
            total_bytes_reclaimed += bytes_reclaimed
        l.info('Trimmed %s partitions removing approximately %s rows and reclaiming %s bytes',
               partitions_trimmed, total_rows_trimmed, total_bytes_reclaimed)
        l.debug('Updating rolled_on')
        cursor.execute('UPDATE rolling_window.maintained_table SET rolled_on = transaction_timestamp() WHERE relid = %(relid)s RETURNING transaction_timestamp()',
                       {'relid': self.relid})
        self._rolled_on = cursor.fetchone()[0]

    class Partition(object):
        def __init__(self, child_name, estimated_rows, total_relation_size_in_bytes):
            self._child_name = child_name
            self._estimated_rows = estimated_rows
            self._total_relation_size_in_bytes = total_relation_size_in_bytes

        @property
        def child_name(self):
            return self._child_name

        @property
        def estimated_rows(self):
            return self._estimated_rows

        @property
        def total_relation_size_in_bytes(self):
            return self._total_relation_size_in_bytes

        def __cmp__(self, other):
            if self.child_name != other.child_name:
                return cmp(self.child_name, other.child_name)
            if self.estimated_rows != other.estimated_rows:
                return cmp(self.estimated_rows, other.estimated_rows)
            return cmp(self.total_relation_size_in_bytes, other.total_relation_size_in_bytes)

    def partitions(self):
        'Provide a list of partitions that are part of this table, including bytesize and estimated rowcount.'
        l = getLogger('RollingWindow.list')
        l.debug('Listing %s.%s', self.schema, self.table)
        if not self.is_managed:
            raise UsageError('Can not list partitions of a table that is not managed.')
        cursor = self.db.connection.cursor()
        cursor.execute('SELECT relname, floor(reltuples) AS reltuples, total_relation_size_in_bytes FROM rolling_window.list_partitions(%(schema)s, %(table)s) ORDER BY relname',
                       {'schema': self.schema, 'table': self.table})
        for r in cursor.fetchall():
            p = self.Partition(*r)
            l.debug('%s.%s has partition %s with approximately %s rows at %s bytes',
                    self.schema, self.table,
                    p.child_name, p.estimated_rows, p.total_relation_size_in_bytes)
            yield p

    class FrozenPartition(object):
        def __init__(self, partition_table_name, new_constraint):
            self._partition_table_name = partition_table_name
            self._new_constraint = new_constraint

        @property
        def partition_table_name(self):
            return self._partition_table_name

        @property
        def new_constraint(self):
            return self._new_constraint

        def __cmp__(self, other):
            if self.partition_table_name != other.partition_table_name:
                return cmp(self.partition_table_name, other.partition_table_name)
            return cmp(self.new_constraint, other.new_constraint)

    def freeze(self):
        """For all but the highest non-empty partition, add a min/max bound constraint for each freeze_column.

        Min and Max values are determined by scanning the table at freeze time.
        """
        l = getLogger('RollingTable.freeze')
        l.debug('Freezing %s.%s', self.schema, self.table)
        if not self.is_managed:
            raise UsageError('Can not freeze partitions of a table that is not managed.')
        cursor = self.db.connection.cursor()
        cursor.execute('SELECT partition_table_name, new_constraint FROM rolling_window.freeze(%(schema)s, %(table)s)',
            {'schema': self.schema, 'table': self.table})
        for r in cursor.fetchall():
            p = self.FrozenPartition(r[0], r[1])
            l.debug('Partition %s added constraints %s', p.partition_table_name, p.new_constraint)
            yield p

    def update_insert_rule(self):
        raise NotImplementedError('Management of insert rules on the parent is not implemented, yet. Wanna write it?')


##########################################################################
class MaintainedTables(object):
    def __init__(self, db):
        l = getLogger('MaintaintedTables.init')
        l.debug('init connection: %s', db)
        self.db = db

    def __iter__(self):
        l = getLogger('MaintainedTables.iter')
        cursor = self.db.connection.cursor()
        cursor.execute("""
SELECT n.nspname, c.relname
FROM rolling_window.maintained_table m
INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
ORDER BY 1, 2
""")
        self.maintained_tables = cursor.fetchall()
        return self

    def next(self):
        l = getLogger('MaintainedTables.next')
        l.debug('next maintainted table')
        if len(self.maintained_tables) > 0:
            return self.maintained_tables.pop(0)
        raise StopIteration


##########################################################################
# End of Library section
##########################################################################

##########################################################################
def add(options):
    l = getLogger('add')
    l.debug('adding')
    missing_a_parameter = False
    for p in ['table', 'partition_column', 'step', 'partition_retention', 'partition_ahead']:
        if p not in dir(options):
            l.error('Required parameter for add action is missing: %s', p)
            missing_a_parameter = True
            continue
        x = eval('options.%s' % p)
        if x is None:
            l.error('Required parameter for add action is missing: %s', p)
            missing_a_parameter = True
            continue
    if missing_a_parameter:
        return -1
    t = RollingTable(options.db, options.schema, options.table)
    if t.is_managed:
        l.error('%s.%s is already managed. Stopping.', options.schema, options.table)
    t.add(options.partition_column, options.step,
          options.partition_retention, options.partition_ahead,
          options.freeze_columns, options.data_lag_window)

##########################################################################
def roll(options):
    l = getLogger('roll')
    l.debug('rolling')
    if options.table is not None:   # I'm rolling a single table
        t = RollingTable(options.db, options.schema, options.table)
        if not t.is_managed:
            l.error('%s.%s is not managed. Stopping.', options.schema, options.table)
        t.roll()
    else:   # I'm rolling all the tables under management.
        m = MaintainedTables(options.db)
        for managed_table in m:
            t = RollingTable(options.db, managed_table[0], managed_table[1])
            t.roll()

##########################################################################
def bytes_to_human(bytes):
    bytes = float(bytes)
    prefixes = ['B', 'kB', 'MB', 'GB', 'TB', 'PB']
    while (bytes / 1024) > 1:
        bytes = bytes / 1024
        prefixes.pop(0)
    return '%.1f %s' % (bytes, prefixes.pop(0))

##########################################################################
def list_table(db, schema, table):
    #TODO: list in order by partition name.
    l = getLogger('list_table')
    t = RollingTable(db, schema, table)
    if not t.is_managed:
        l.error('%s.%s is not managed. Stopping.', schema, table)
        return
    print 'Table %s.%s %s with about %d rows last rolled %s' % ( schema, table, bytes_to_human(t._parent_total_relation_size_in_bytes), t.parent_estimated_rows, t.rolled_on )
    partition_count = 0
    sum_of_estimated_rows = t.parent_estimated_rows
    sum_of_total_relation_size_in_bytes = t.parent_total_relation_size_in_bytes
    for p in t.partitions():
        partition_count += 1
        sum_of_estimated_rows += p.estimated_rows
        sum_of_total_relation_size_in_bytes += p.total_relation_size_in_bytes
    for p in t.partitions():
        print '  %s: %s with about %d rows.' % (p.child_name, bytes_to_human(p.total_relation_size_in_bytes), p.estimated_rows)
    print 'Total of %d partitions consuming %s with a total of about %d rows.' % (partition_count, bytes_to_human(sum_of_total_relation_size_in_bytes), sum_of_estimated_rows)

##########################################################################
def list(options):
    l = getLogger('list')
    l.debug('listing')
    if options.table is not None:   # I'm rolling a single table
        list_table(options.db, options.schema, options.table)
    else:
        m = MaintainedTables(options.db)
        for managed_table in m:
            list_table(options.db, managed_table[0], managed_table[1])

##########################################################################
def freeze_table(db, schema, table):
    l = getLogger('freeze_table')
    l.debug('Freezing %s.%s', schema, table)
    t = RollingTable(db, schema, table)
    if not t.is_managed:
        l.error('%s.%s is not managed. Stopping.', schema, table)
        return
    for f in t.freeze():
        print 'Partition %s.%s added %s' % (schema, f.partition_table_name, f.new_constraint)

##########################################################################
def freeze(options):
    l = getLogger('freeze')
    l.debug('Freezing')
    if options.table is not None:   # I'm rolling a single table
        freeze_table(options.db, options.schema, options.table)
    else:
        l.debug('No table specified. Freeze them all.')
        m = MaintainedTables(options.db)
        for managed_table in m:
            freeze_table(options.db, managed_table[0], managed_table[1])


##########################################################################
# Interactive commands
actions = {
    'add': add,
    'roll': roll,
    'list': list,
    'freeze': freeze,
    # pack
    # ship
    # unpack
}

def main():
    usage="""usage: %prog [list|add|roll|freeze] ...
List tables under management (or details about a specific table with the table parameter):
    list [-t <table>] [<PostgreSQL options>]

Adds table to the rolling window system for maintenance.
    add -t <table> -c <partition_column> -s <step> -r <retention> -a <advanced> [-f <freeze column> [-f ...]] [<PostgreSQL options>]

Roll the table (or all maintained tables if no table parameter):
    roll [-t <table>] [<PostgreSQL options>]

Freeze all but the highest non-empty partition for the table (or all maintained tables if no table parameter):
    freeze [-t <table>] [<PostgreSQL options>]

Note that for PostgreSQL options, standard libpq conventions are followed.
See http://www.postgresql.org/docs/current/static/libpq-envars.html')
"""

    # TODO: support addition of new freeze columns?

    from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, StreamHandler
    parser = OptionParser(usage=usage, version=__vcs_id__, conflict_handler="resolve")
    parser.add_option('-q', '--quiet', dest='quiet_count', action='count')
    parser.add_option('-v', '--verbose', dest='verbose_count', action='count')

    parser.add_option('-t', '--table',
        help='required for add: act on this particular table')
    parser.add_option('-n', '--schema', default='public',
        help='... in this particular schema, defaulting to public')
    parser.add_option('-c', '--partition_column', dest='partition_column',
        help='column to use as a partition key, required')
    parser.add_option('-s', '--step',
        help='partition width in terms of the partition column (lower_bound + step - 1 = upper_bound)')
    parser.add_option('-r', '--partition_retention', dest='partition_retention',
        help='target number of non-empty partitions to keep around, the width of the window')
    parser.add_option('-a', '--partition_ahead', dest='partition_ahead',
        help='target number of empty reserve partitions to keep ahead of the window')
    parser.add_option('-f', '--freeze_columns', action='append', default=[],
        help='columns to be constrained when partitions are frozen')
    parser.add_option('-l', '--data_lag_window', default=0,
        help='partitions following the highest partition with data to hold back from freezing / dumping')

    postgres_group = OptionGroup(parser, 'PostgreSQL connection options')
    postgres_group.add_option('-h', '--host',
        help='Specifies the host name of the machine on which the server is running. If the value begins with a slash, it is used as the directory for the Unix-domain socket.')
    postgres_group.add_option('-d', '--dbname', dest='database',
        help='Specifies the name of the database to connect to.')
    postgres_group.add_option('-p', '--port',
        help="""Specifies the TCP port or the local Unix-domain socket file extension on which the server is listening for connections. Defaults to the value of the PGPORT environment variable or, if not set, to the port specified at compile time, usually 5432.""")
    postgres_group.add_option('-U', '--username',
        help='Connect to the database as the user username instead of the default. (You must have permission to do so, of course.)')
    postgres_group.add_option('-w', '--no-password', dest='password_prompt', action='store_false',
        help='Never issue a password prompt. NOT SUPPORTED.')
    postgres_group.add_option('-W', '--password', dest='password_prompt', action='store_true',
        help='Force prompt for a password. NOT SUPPORTED.')
    parser.add_option_group(postgres_group)

    (options, args) = parser.parse_args()
    l = getLogger()
    console = StreamHandler()
    l.addHandler(console)
    raw_log_level = 2   # default to warn level
    if options.verbose_count is not None: raw_log_level += options.verbose_count
    if options.quiet_count is not None:   raw_log_level -= options.quiet_count
    if   raw_log_level <= 0: l.setLevel(CRITICAL)
    elif raw_log_level == 1: l.setLevel(ERROR)
    elif raw_log_level == 2: l.setLevel(WARNING)    # default
    elif raw_log_level == 3: l.setLevel(INFO)
    else:                    l.setLevel(DEBUG)

    action = actions.get(args.pop(0), None)
    if action is None:
        return parser.print_help()
    else:
        options.ensure_value('db', PgConnection(options))
        return action(options)

if __name__ == '__main__':
    main()
