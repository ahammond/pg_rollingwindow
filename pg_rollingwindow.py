#!/usr/bin/env python
from datetime import datetime, timedelta
from functools import wraps
from logging import getLogger
from optparse import OptionParser, OptionGroup
from os import access, environ, listdir, rename, R_OK, W_OK
from os.path import exists, isdir, isfile
from os.path import join as path_join
from os.path import split as path_split
from psycopg2 import connect, IntegrityError, ProgrammingError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED
import re
from shlex import split as shlex_split
from subprocess import call, check_call
from time import sleep

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

    To share connections with other parts of the code,
    simply pass in the connection and a None for the options.
    This isn't a multi-treaded application. Keep it simple.
    """
    legal_arguments = ('database', 'username', 'password', 'host', 'port', 'sslmode' )

    def __init__(self, options, connection=None):
        l = getLogger('PgConnection.__init__')
        l.debug('init')
        self._connection = connection
        self.arguments = {}
        for k in dir(options):
            if k in self.legal_arguments:
                v = eval('options.%s' % k)
                if 'username' == k:
                    k = 'user'
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
class PgToolCaller(object):
    legal_pg_arguments = ('username', 'host', 'port', 'sslmode' )   # database parameter is handled specially because of pg_dump

    def __init__(self, options, connection=None):
        """Create a partition dumper helper object.

        Used to dump and restore files, and importantly to figure out which are eligible or not.

        NOTE: The code will piggy-back on the optional connection if provided when instantiating
        RollingTable objects for return.
        BUT the options passed must still include any database connection options required since
        we dump and restore by calling pg_dump and pg_restore, and they both need these parameters.
        """
        l = getLogger('PartitionDumper.__init__')

        if connection:
            self.db = PgConnection(options, connection)
        else:
            self.db = options.db

        self.pg_path = None
        if 'PGPATH' in environ:
            self.pg_path = environ['PGPATH']
        if 'pg_path' in dir(options):
            self.pg_path = options.pg_path

        self.database = None
        if 'database' in dir(options):
            self.database = options.database

        self.database_connection_arguments = []     # remember, PG environment variables can also provide this info.
        for k in self.legal_pg_arguments:
            if k in dir(options):
                v = eval('options.%s' % k)
                if v is not None:
                    self.database_connection_arguments.append('--%s=%s' % (k,v))

    def get_binary_path(self, binary):
        return binary if self.pg_path is None else path_join(self.pg_path, binary)


##########################################################################
class SchemaInitializer(PgToolCaller):

    def initialize_schema(self):
        """Load the rolling_window schema, tables and supporting functions from pg_rollingwindow_api.sql

        Calls psql to import the pg_rollingwindow_api.sql file.
        Assumes that pg_rollingwindow_api.sql is in the same directory that this file is in.
        """

        l = getLogger('SchemaInitializer.initialize_schema')

        directory = path_split(__file__)[0]
        pg_rollingwindow_api_full_path = path_join(directory, 'pg_rollingwindow_api.sql')

        load_command = []
        load_command.append(self.get_binary_path('psql'))
        load_command.extend(self.database_connection_arguments)
        load_command.append('--file=%s' % pg_rollingwindow_api_full_path)
        if self.database is not None:
            load_command.append(self.database)

        l.debug('load_command = %r', load_command)
        if call(load_command):
            raise RuntimeError('psql failed!')


##########################################################################
class EligiblePartition(object):
    def __init__(self, name, schema_only=False):
        self.name = name
        self.schema_only = schema_only

    def __repr__(self):
        return 'EligiblePartition("%s",%s)' % (self.name, self.schema_only)

    def __cmp__(self, other):
        if self.name == other.name:
            return cmp(self.schema_only, other.schema_only)
        return cmp(self.name, other.name)


##########################################################################
class PartitionDumper(PgToolCaller):

    standard_pg_dump_arguments = ['--format=custom', '--compress=9', '--no-password']
    standard_pg_restore_arguments = ['--format=custom', '--jobs=4', '--no-password', '--exit-on-error' ]
    # TODO: handle passwords? This would involve talking to pg_dump after it's been started.
    # That's a little more complicated... next revision.
    # For now require an appropriate pg_hba.conf or .pgpass entries.

    def __init__(self, options, connection=None):
        if 'dump_directory' not in dir(options):
            raise UsageError('Dumpers must be instantiated with a dump_directory option')
        self.dump_directory = options.dump_directory
        super(PartitionDumper, self).__init__(options, connection)

    def validate_dump_directory(self, write_access=False):
        l = getLogger('PartitionDumper.validate_dump_directory')
        if not exists(self.dump_directory):
            l.error('dump_directory does not exist: %s', self.dump_directory)
            raise UsageError('dump_directory %s does not exist' % self.dump_directory)
        if not isdir(self.dump_directory):
            l.error('dump_directory is not a directory: %s', self.dump_directory)
            raise UsageError('dump_directory %s is not a directory' % self.dump_directory)
        if not access(self.dump_directory, R_OK):
            l.error('dump_directory is not readable: %s', self.dump_directory)
            raise UsageError('dump_directory %s is not readable' % self.dump_directory)
        if write_access:
            if not access(self.dump_directory, W_OK):
                raise UsageError('dump_directory %s is not writeable' % self.dump_directory)

    def partition_pattern(self, r):
        """Partitions should look like basetable_0000123

        In other words, the base table's name, followed by an underscore, followed by a 0 prefixed decimal number.
        """
        return re.compile(r'^%s.*_(?P<number>\d+)$' % re.escape(r.table))

    def dump_table(self, r, partition_name, schema_only=False):
        """Dump the partition_name table, belonging to the RollingWindow described by r.
        """

        #TODO: really clever would be to always schema-only dump the parent partition, so that restore always has it handy.

        l = getLogger('PartitionDumper.dump_table')
        self.validate_dump_directory(write_access=True)
        dump_file = path_join(self.dump_directory, '%s.%s.pgdump' % (r.schema, partition_name))
        partial_dump_file = dump_file + '.partial'
        l.debug('Dumping %s.%s partition %s to %s', r.schema, r.table, partition_name, partial_dump_file)
        #TODO: add pipes to both stdout and stderr and capture for l.debug() / cleanlieness???

        dump_command = []
        dump_command.append(self.get_binary_path('pg_dump'))
        dump_command.extend(self.database_connection_arguments)
        dump_command.extend(self.standard_pg_dump_arguments)
        dump_command.append('--file=%s' % (partial_dump_file,))
        dump_command.append('--table=%s.%s' % (r.schema, partition_name))
        if schema_only:
            dump_command.append('--schema-only')
        if self.database is not None:
            dump_command.append(self.database)

        l.debug('dump_command = %r', dump_command)
        if call(dump_command):
            raise RuntimeError('pg_dump failed!')   # uh, is this a reasonable exception to use?

        # The usual case is that we're dumping a child table and should keep track,
        # but... don't barf in situations where that's not the case.
        m = self.partition_pattern(r).match(partition_name)
        if m:
            r.last_partition_dumped = int(m.group('number'))
        l.debug('Renaming %s to %s', partial_dump_file, dump_file)
        rename(partial_dump_file, dump_file)
        return dump_file

    def eligible_to_dump(self, r):
        """Given a RollingWindow, list the partitions eligible to be dumped.

        A partition is eligible if it hasn't been dumped before (> r.last_partition_dumped)
        and it meets the critera for being frozen (is r.data_lag_window behind the highest non-empty partition)

        Special case is when last_partition_dumped is -1 (table has never been dumped)
        In this case, we first return the parent table, but schema only.
        This is based on the assumption that data will be moved from the parent to a partition.
        """

        l = getLogger('PartitionDumper.dump')
        highest_freezable = r.highest_freezable
        l.debug('Examining %s.%s for dumping. Highest freezable partition is %s', r.schema, r.table, highest_freezable)

        # has table ever been dumped before?
        if -1 == r.last_partition_dumped:
            l.info('Parent table is eligible')
            yield EligiblePartition(r.table, schema_only=True)

        for p in r.partitions():
            l.debug('Considering %s for eligibility', p.partition_table_name)
            if p.partition_table_name.endswith('_limbo'):
                l.debug('Skipping limbo partition')
                continue
            match = self.partition_pattern(r).match(p.partition_table_name)
            if match is None:
                l.warning('Skipping strange partition: %s', p.partition_table_name)
                continue
            partition_number = int(match.group('number'))
            if partition_number <= r.last_partition_dumped:
                l.debug('I think I already dumped %s since %d is less than %d', p.partition_table_name, partition_number, r.last_partition_dumped)
                continue
            if not p.is_frozen:
                l.debug('Skipping %s because it is freezable but is not yet frozen', p.partition_table_name)
                continue
            #TODO: what about checking to see if a dumpfile or partial already exists? (no clobber)
            l.debug('%s looks eligible', p.partition_table_name)
            yield EligiblePartition(p.partition_table_name, schema_only=False)
            #TODO: would it be better to fork these off in parallel?
            # That would make keeping track of last_partition_dumped a little trickier. What if one fails?
            # I think the solution will involve parallel pg_dump, which should be in pg 9.2. So, hold off until then.

    def restore_file(self, r, filename):
        l = getLogger('PartitionDumper.restore_file')
        self.validate_dump_directory()
        l.debug('Restoring %s.%s from %s', r.schema, r.table, filename)
        restore_command = []
        restore_command.append(self.get_binary_path('pg_restore'))
        restore_command.extend(self.database_connection_arguments)
        restore_command.extend(self.standard_pg_restore_arguments)
#        restore_command.extend(['--schema=%s' % r.schema ,'--table=%s' % r.table])      # it seems pointless to limit this. Why would random stuff be in the restore file?
        if self.database is not None:
            restore_command.append('--dbname=%s' % self.database)
        else:
            l.warning('No database specified. Restore does not default to PGDATABASE. Piping to STDOUT.')
        restore_command.append(filename)    # filename must be the last argument.

        l.debug('restore_command = %r', restore_command)
        if call(restore_command):
            raise RuntimeError('pg_restore failed!')
        # should I return the partition name? How do I know what was actually restored? Better return nothing for now.

    def files_eligible_to_restore(self, r):
        """Given a RollingWindow, list files eligible for loading.

        A file is eligible if the partition it contains hasn't already been loaded
        and a complete copy of it is available in the dump directory (skip .partials).
        """

        l = getLogger('PartitionDumper.restore')
        self.validate_dump_directory()
        l.debug('Finding dumps for %s.%s in %s', r.schema, r.table, self.dump_directory)

        partition_pattern = self.partition_pattern(r)
        already_loaded_list = []
        for p in r.partitions():
            m = partition_pattern.match(p.partition_table_name)
            if m:
                already_loaded_list.append(int(m.group('number')))
        already_loaded_partition_numbers = frozenset(already_loaded_list)

        potential_dump_files = listdir(self.dump_directory)

        # If the parent can be loaded and needs to be loaded, always report it as eligible first.
        parent_dump_file_name = '%s.%s.pgdump' % (r.schema, r.table)
        # a relid of None means the table not only isn't managed, but doesn't even exist in the database.
        if r.relid is None and parent_dump_file_name in potential_dump_files:
            full_parent_dump_file = path_join(self.dump_directory, parent_dump_file_name)
            if not isfile(full_parent_dump_file):
                l.debug('Skipping %s as not a file.', parent_dump_file_name)
            else:
                l.debug('Eligible to restore %s, which is the parent table.', parent_dump_file_name)
                potential_dump_files.remove(parent_dump_file_name)
                yield full_parent_dump_file

        file_pattern = re.compile(r'^%s.%s_(?P<number>\d+).pgdump$' % (re.escape(r.schema), re.escape(r.table)))
        # TODO: use pg_restore --list to figure out what tables / partitions are actually in each dumpfile?
        for dump_file in potential_dump_files:
            full_path = path_join(self.dump_directory, dump_file)
            if not isfile(full_path):
                l.debug('Skipping %s as not a file.', dump_file)
                continue
            m = file_pattern.match(dump_file)
            if m is None:
                l.debug('Skipping %s as it does not match the pattern of dumps for %s.%s.', dump_file, r.schema, r.table)
                continue
            if int(m.group('number')) in already_loaded_partition_numbers:
                l.debug('Skipping %s as that partition is already loaded.', dump_file)
                continue
            l.debug('Eligible to restore %s', dump_file)
            yield full_path

        # TODO: multiple parallel psql's for load? No, that would be stupid. Instead work with pg_restore and parallel restore.
        # locking issues around the creation of multiple partitions of the same parent?
        # Probably not: create table is the only thing that really requires such...

    def tables_eligible_to_restore(self):
        """Given a dump_directory, list tables with dump files present (in the form of RollingWindows)

        If you have managed to sneak a . into your schema name, this will break.
        Also, tables with _ in their name must have a non-digit following the underscore.
        for example, schema_foo.table_1bar will break illegal.
        """
        l = getLogger('PartitionDumper.restore')
        self.validate_dump_directory()
        l.debug('Scanning %s for dump files', self.dump_directory)
        already_returned = []
        partition_file_pattern = re.compile(r'^(?P<schema>[^.]+)\.(?P<table>(:?[^._]+|_[^.\d])+)_(?P<partition_number>\d{19})\.pgdump$')
        parent_file_pattern = re.compile(r'^(?P<schema>[^.]+)\.(?P<table>[^.]+?)\.pgdump$')
        for file_name in listdir(self.dump_directory):
            full_name = path_join(self.dump_directory, file_name)
            l.debug('Considering %s for restore eligibility', file_name)
            if not isfile(full_name):
                l.debug('Skipping %s as not a file', file_name)
                continue
            m = partition_file_pattern.match(file_name)
            if not m:   # the usual case is a partition file, but... it might be a parent table.
                l.debug('Did not match partition_file_pattern: %s', file_name)
                m = parent_file_pattern.match(file_name)
            if not m:
                l.debug('Did not match parent_file_pattern either: %s', file_name)
                continue
            r = RollingWindow(self.db, m.group('schema'), m.group('table'))
            if r in already_returned:
                l.debug('Already returned table for %s', file_name)
                continue
            already_returned.append(r)
            yield r


##########################################################################
class PartitionResult(object):
    def __init__(self, method, partition_table_name, rows_moved):
        self.method = method
        self.partition_name = partition_table_name
        self.rows_moved = rows_moved


##########################################################################
class Partition(object):
    R_PARTITION = re.compile(r'^(?P<parent_name>.*)_(?:(?P<lower_bound>\d+)|(?P<is_limbo>limbo))$')

    def __init__(self, partition_table_name, estimated_rows, total_relation_size_in_bytes, is_frozen):
        self.partition_table_name = partition_table_name
        self.estimated_rows = estimated_rows
        self.total_relation_size_in_bytes = total_relation_size_in_bytes
        self.is_frozen = is_frozen
        m = self.R_PARTITION.match(partition_table_name)
        self.lower_bound = None
        self.is_limbo = False
        if not m:
            return
        if m.group('is_limbo') == 'limbo':
            self.is_limbo = True
            return
        self.lower_bound = int(m.group('lower_bound'))

    def __cmp__(self, other):
        if self.partition_table_name != other.partition_table_name:
            return cmp(self.partition_table_name, other.partition_table_name)
        if self.estimated_rows != other.estimated_rows:
            return cmp(self.estimated_rows, other.estimated_rows)
        return cmp(self.total_relation_size_in_bytes, other.total_relation_size_in_bytes)


##########################################################################
class FrozenPartition(object):
    def __init__(self, partition_table_name, new_constraint):
        self.partition_table_name = partition_table_name
        self.new_constraint = new_constraint

    def __cmp__(self, other):
        if self.partition_table_name != other.partition_table_name:
            return cmp(self.partition_table_name, other.partition_table_name)
        return cmp(self.new_constraint, other.new_constraint)

    def __repr__(self):
        return 'FrozenPartition(%s, %s)' % (self.partition_table_name, self.new_constraint)


##########################################################################
class RollingWindow(object):
    """Encapsulate handling of a table and it's partitions.

    """
    CREATED = 'c'
    MOVED = 'm'

    def __init__(self, db, schema, table):
        l = getLogger('RollingWindow.init')
        l.debug('RollingWindow: %s, %s', schema, table)
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
        self.rolls_since_last_vacuum = 0

    def __repr__(self):
        return "<RollingWindow('%s', '%s')>" % (self.schema, self.table)

    def __cmp__(self, other):
        if self.schema == other.schema:
            return cmp(self.table, other.table)
        return cmp(self.schema, other.schema)

    def fetch(self):
        l = getLogger('RollingWindow.fetch')
        l.debug('fetching %s.%s', self.schema, self.table)
        cursor = self.db.connection.cursor()
        try:
            cursor.execute('SET search_path TO %(schema)s', {'schema': self.schema})
        except ProgrammingError as e:
            if e.pgcode == '3F000' :    # schema does not exist
                self._is_managed = False
                return
            else:
                raise
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
        if self.is_managed:         # implicitly calls fetch
            return int(self._relid)
        cursor = self.db.connection.cursor()
        cursor.execute("""
SELECT c.oid
FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
WHERE c.relname = %(table)s
  AND n.nspname = %(schema)s
""", {'schema': self.schema, 'table': self.table})
        results = cursor.fetchall()
        if len(results) < 1:
            return None
        self._relid = results[0][0]
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
    def data_lag_window(self):
        if not self.is_managed:
            raise UsageError('Table %s.%s either does not exist or is not managed. It must be added before it can be fetched.', self.schema, self.table)
        return self._data_lag_window

    @property
    def last_partition_dumped(self):
        if not self.is_managed:
            raise UsageError('Table %s.%s either does not exist or is not managed. It must be added before it can be fetched.', self.schema, self.table)
        return self._last_partition_dumped

    @last_partition_dumped.setter
    def last_partition_dumped(self, value):
        if not self.is_managed:
            raise UsageError('Table %s.%s either does not exist or is not managed. It must be added before it can be fetched.', self.schema, self.table)
        cursor = self.db.connection.cursor()
        cursor.execute("""
UPDATE rolling_window.maintained_table
SET last_partition_dumped = %(last_partition_dumped)s
FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
WHERE rolling_window.maintained_table.relid = c.oid
  AND c.relname = %(table)s
  AND n.nspname = %(schema)s
""", {'schema': self.schema, 'table': self.table,
      'last_partition_dumped': value})
        if cursor.rowcount < 1:
            raise UsageError('Update of last_partition_dumped failed. Why?')
        if cursor.rowcount > 1:
            raise UsageError('Update of last_partition_dumped hit %d rows... it should not be possible to update more than 1 row!!!' % cursor.rowcount)
        self._last_partition_dumped = value

    def add(self, attname, step,
            non_empty_partitions_to_keep, reserve_partitions_to_keep,
            data_lag_window):
        """Add a table to the list of tables under management."""
        l = getLogger('RollingWindow.add')
        l.debug('Adding %s.%s', self.schema, self.table)
        if self.is_managed:
            raise UsageError('May not add a table which is already managed.')
        self._attname = attname
        self._step = step
        self._non_empty_partitions_to_keep = non_empty_partitions_to_keep
        self._reserve_partitions_to_keep = reserve_partitions_to_keep
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
        cursor.execute('SELECT rolling_window.add_limbo_partition(%(schema)s, %(table)s)',
            {'schema': self.schema, 'table': self.table})
        if cursor.rowcount < 1:
            raise UsageError('Limbo table not created? Why not?!')
        self._is_managed = True
        self.db.connection.commit()
        self.db.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def freeze_column(self, column, overlap=None, prior_upper_bound_percentile=None):
        l = getLogger('RollingWindow.freeze_column')
        if overlap is None:
            l.debug('Freezing column %s.%s.%s with no overlap bounary',
                    self.schema, self.table, column)
        else:
            if prior_upper_bound_percentile is None:
                l.debug('Freezing column %s.%s.%s with overlap bounary %s',
                        self.schema, self.table, column, overlap)
            else:
                l.debug('Freezing column %s.%s.%s with overlap bounary %s based on %dth percentile',
                        self.schema, self.table, column, overlap, prior_upper_bound_percentile)

        if not self.is_managed:
            raise UsageError('May not set a freeze column on a table which is not managed.')
        cursor = self.db.connection.cursor()
        cursor.execute('SELECT rolling_window.set_freeze_column(%(relid)s, %(column_name)s, %(lower_bound_overlap)s, %(prior_upper_bound_percentile)s)',
                       {'relid': self.relid, 'column_name': column, 'lower_bound_overlap': overlap,
                        'prior_upper_bound_percentile': prior_upper_bound_percentile})
        result = cursor.fetchone()[0]
        if result:
            l.debug('Updated freeze column settings.')
        else:
            l.debug('Created freeze column settings.')
        return result

    def partition(self, clone_indexes=True, vacuum_parent_after_every=0):
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
            self.rolls_since_last_vacuum += 1
            if 0 < vacuum_parent_after_every < self.rolls_since_last_vacuum:
                self.rolls_since_last_vacuum = 0
                parameters = {'schema': self.schema, 'table': self.table}
                l.debug('Running VACUUM FULL on %(schema)s.%(table)s' % parameters)
                cursor.execute('VACUUM FULL %(schema)s.%(table)s' % parameters)     # Is there a polite way to get these identifiers quoted?
            yield PartitionResult(self.CREATED, partition_created, rows_moved)
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
                try:
                    # Do _NOT_ clone_indexes here. Instead do it in a separate transaction.
                    cursor.execute('SELECT rolling_window.move_data_to_partition(%(schema)s, %(table)s, %(lower_bound)s, %(clone_indexes)s, %(to_limbo)s)',
                                   {'schema': self.schema, 'table': self.table, 'lower_bound': lower_bound,
                                    'clone_indexes': False, 'to_limbo': False})
                    rows_moved = cursor.fetchone()[0]
                    l.info('Moved %s rows to partition %s.', rows_moved, partition)
                except IntegrityError, e:
                    l.warn('Failed to move data to partition for %s.%s %s: %s', self.schema, self.table, lower_bound, e)
                    #TODO: a more fine-grained approach around sending data to limbo would be nice. This sends entire chunks of data...
                    cursor.execute('SELECT rolling_window.move_data_to_partition(%(schema)s, %(table)s, %(lower_bound)s, %(clone_indexes)s, %(to_limbo)s)',
                        {'schema': self.schema, 'table': self.table, 'lower_bound': lower_bound,
                         'clone_indexes': False, 'to_limbo': True})
                    rows_moved = cursor.fetchone()[0]
                    l.info('Moved %s rows to %s.%s_limbo', rows_moved, self.schema, self.table)
                self.rolls_since_last_vacuum += 1
                if 0 < vacuum_parent_after_every < self.rolls_since_last_vacuum:
                    self.rolls_since_last_vacuum = 0
                    parameters = {'schema': self.schema, 'table': self.table}
                    l.debug('Running VACUUM FULL on %(schema)s.%(table)s' % parameters)
                    cursor.execute('VACUUM FULL %(schema)s.%(table)s' % parameters)
                cursor.execute('SELECT new_index FROM rolling_window.clone_indexes_to_partition(%(schema)s, %(table)s, %(lower_bound)s) AS citp(new_index)',
                        {'schema': self.schema, 'table': self.table, 'lower_bound': lower_bound})
                new_indexes = [x[0] for x in cursor.fetchall()]
                l.debug('Added %d indexes to %s.%s: %r', cursor.rowcount, self.schema, self.table, new_indexes)
                yield PartitionResult(self.MOVED, partition, rows_moved)

    def roll(self, vacuum_parent_after_every=0):
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
        for x in self.partition(vacuum_parent_after_every=vacuum_parent_after_every):
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

    def partitions(self, descending=False, with_size=False):
        """Provide a list of partitions that are part of this table,
        including estimated rowcount and, if requested, the bytesize.

        If descending=True then return them in descending order.
        If with_size=True then include the total_relation_size_in_byte, otherwise set this to None
        """
        #TODO: add only_windows=False option to only return stuff that looks like foo_0000123?

        l = getLogger('RollingWindow.list')
        l.debug('Listing %s.%s', self.schema, self.table)
        cursor = self.db.connection.cursor()
        if with_size:
            query = 'SELECT relname, floor(reltuples) AS reltuples, pg_total_relation_size(partition_table_oid) AS total_relation_size_in_bytes, is_frozen FROM rolling_window.list_partitions(%(schema)s, %(table)s) ORDER BY relname'
        else:
            query = 'SELECT relname, floor(reltuples) AS reltuples, NULL AS total_relation_size_in_bytes, is_frozen FROM rolling_window.list_partitions(%(schema)s, %(table)s) ORDER BY relname'
        if descending:
            query += ' DESCENDING'
        cursor.execute(query, {'schema': self.schema, 'table': self.table})
        for r in cursor.fetchall():
            p = Partition(*r)
            l.debug('%s.%s has partition %s with approximately %s rows at %s bytes',
                    self.schema, self.table,
                    p.partition_table_name, p.estimated_rows, p.total_relation_size_in_bytes)
            yield p

    @property
    def highest_freezable(self):
        l = getLogger('RollingWindow.highest_freezable')
        if not self.is_managed:
            raise UsageError('Can not determine highest freezeable partition of a table that is not managed.')
        cursor = self.db.connection.cursor()
        l.debug('Getting highest_freezable for %s.%s', self.schema, self.table)
        cursor.execute('SELECT rolling_window.highest_freezable(%(schema)s, %(table)s)',
             {'schema': self.schema, 'table': self.table})
        r = cursor.fetchone()[0]
        l.debug('Got highest_freezable: %s', r)
        return r

    def freeze(self, cluster=False, vacuum_freeze=False):
        """For all but the highest non-empty partition, add a min/max bound constraint for each freeze_column.

        Clustering is optional and defaults to off as it's expensive.
        If used, should minimize disk space usage and speed up sequential scans, etc.
        Min and Max values are determined by scanning the table at freeze time.
        """
        l = getLogger('RollingWindow.freeze')
        l.debug('Freezing %s.%s', self.schema, self.table)
        if not self.is_managed:
            raise UsageError('Can not freeze partitions of a table that is not managed.')
        cursor = self.db.connection.cursor()
        h = self.highest_freezable
        for p in self.partitions():
            l.debug('Considering %s.%s for freezing.', self.schema, p.partition_table_name)
            if p.is_limbo or p.lower_bound is None:
                l.debug('Skipping partition %s.%s since is_limbo or no lower_bound.', self.schema, p.partition_table_name)
                continue
            if p.is_frozen:
                l.debug('Skipping partition %s.%s since all freezable columns already have bound constraints.', self.schema, p.partition_table_name)
                continue
            if p.partition_table_name > h:
                l.debug('Highest freezable is %s. Stopping.', h)
                break
            parameters = {'schema': self.schema, 'table': self.table, 'lower_bound': p.lower_bound}
            cursor.execute('SELECT rolling_window.move_data_below_lower_bound_overlap_to_limbo(%(schema)s, %(table)s, %(lower_bound)s) AS data_moved_to_limbo', parameters)
            data_moved_to_limbo = cursor.fetchone()[0]
            l.debug('Partition %s.%s moved %d rows to limbo', self.schema, self.table, data_moved_to_limbo)
            cursor.execute('SELECT f.c AS new_constraint FROM rolling_window.freeze_partition(%(schema)s, %(table)s, %(lower_bound)s) AS f(c)', parameters)
            for r in cursor.fetchall():
                f = FrozenPartition(partition_table_name=p.partition_table_name, new_constraint=r[0])
                l.debug('Partition %s added constraints %s', p.partition_table_name, f.new_constraint)
                yield f
            if cluster:
                l.debug('CLUSTERing %s.%s', self.schema, p.partition_table_name)
                cursor.execute('CLUSTER %(schema)s.%(table)s' % {'schema': self.schema, 'table': p.partition_table_name})
            if vacuum_freeze:
                l.debug('VACUUM FREEZEing %s.%s', self.schema, p.partition_table_name)
                cursor.execute('VACUUM FREEZE %(schema)s.%(table)s' % {'schema': self.schema, 'table': p.partition_table_name})

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
def init(options):
    """Initialize database with pg_rollingwindow_api.sql
    """
    l = getLogger('init')
    i = SchemaInitializer(options)
    i.initialize_schema()

##########################################################################
def add(options):
    """Add a table for management under RollingWindow code.
    """
    l = getLogger('add')
    l.debug('adding')
    missing_a_parameter = False
    for p in ['table', 'column', 'step', 'partition_retention', 'partition_ahead']:
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
    t = RollingWindow(options.db, options.schema, options.table)
    if t.is_managed:
        l.error('%s.%s is already managed. Stopping.', options.schema, options.table)
    t.add(options.column,
          options.step,
          options.partition_retention,
          options.partition_ahead,
          options.data_lag_window)

##########################################################################
def roll(options):
    """Roll a table, or all tables under management.
    """
    l = getLogger('roll')
    l.debug('rolling')
    if options.table is not None:   # I'm rolling a single table
        t = RollingWindow(options.db, options.schema, options.table)
        if not t.is_managed:
            l.error('%s.%s is not managed. Stopping.', options.schema, options.table)
        t.roll(options.vacuum_parent_after_every)
    else:   # I'm rolling all the tables under management.
        m = MaintainedTables(options.db)
        for managed_table in m:
            t = RollingWindow(options.db, managed_table[0], managed_table[1])
            t.roll(vacuum_parent_after_every=options.vacuum_parent_after_every)

##########################################################################
def list_table(db, schema, table):
    def bytes_to_human(bytes):
        try:
            bytes = float(bytes)
            prefixes = ['B', 'kB', 'MB', 'GB', 'TB', 'PB']
            while (bytes / 1024) > 1:
                bytes /= 1024
                prefixes.pop(0)
            return '%.1f %s' % (bytes, prefixes.pop(0))
        except TypeError:
            return bytes

    l = getLogger('list_table')
    t = RollingWindow(db, schema, table)
    l.warning('Table %s.%s %s with about %d rows last rolled %s', schema, table, bytes_to_human(t.parent_total_relation_size_in_bytes), t.parent_estimated_rows, t.rolled_on )
    partition_count = 0
    sum_of_estimated_rows = t.parent_estimated_rows
    sum_of_total_relation_size_in_bytes = t.parent_total_relation_size_in_bytes
    for p in t.partitions(with_size=True):
        partition_count += 1
        sum_of_estimated_rows += p.estimated_rows
        sum_of_total_relation_size_in_bytes += p.total_relation_size_in_bytes
        l.info('  %s: %s with about %d rows.', p.partition_table_name, bytes_to_human(p.total_relation_size_in_bytes), p.estimated_rows)
    l.warning('Total of %d partitions consuming %s with a total of about %d rows.', partition_count, bytes_to_human(sum_of_total_relation_size_in_bytes), sum_of_estimated_rows)

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
def freeze_column(options):
    l = getLogger('freeze_column')
    percentile = int(options.prior_upper_bound_percentile) if options.prior_upper_bound_percentile else None
    if options.overlap is None:
        l.debug('Freezing column %s.%s.%s with no overlap',
                options.schema, options.table, options.column)
    else:
        if options.prior_upper_bound_percentile is None:
            l.debug('Freezing column %s.%s.%s with overlap "%s"',
                    options.schema, options.table, options.column, options.overlap)
        else:
            l.debug('Freezing column %s.%s.%s with overlap "%s" based on %dth percentile',
                    options.schema, options.table, options.column, options.overlap, percentile)
    t = RollingWindow(options.db, options.schema, options.table)
    t.freeze_column(options.column, options.overlap, percentile)

##########################################################################
def freeze_table(db, schema, table, cluster, vacuum_freeze):
    l = getLogger('freeze_table')
    l.debug('Freezing %s.%s', schema, table)
    t = RollingWindow(db, schema, table)
    if not t.is_managed:
        l.error('%s.%s is not managed. Stopping.', schema, table)
        return
    for f in t.freeze(cluster, vacuum_freeze):
        l.info('Partition %s.%s added %s', schema, f.partition_table_name, f.new_constraint)

##########################################################################
def freeze(options):
    l = getLogger('freeze')
    l.debug('Freezing')
    if options.table is not None:
        if options.column is not None:  # Configuration mode
            freeze_column(options)
        else:
            freeze_table(options.db, options.schema, options.table, options.cluster, options.freeze_after_cluster)
    else:
        if options.column is not None:
            l.warn('Freeze column defined, but no table defined. If you want to define freeze options on a column, you must also name the table.')
        l.debug('No table specified. Freeze them all.')
        m = MaintainedTables(options.db)
        for managed_table in m:
            freeze_table(options.db, managed_table[0], managed_table[1], options.cluster, options.freeze_after_cluster)

##########################################################################
def dump_table(db, schema, table, dumper):
    l = getLogger('dump_table')
    l.debug('Dumping %s.%s', schema, table)
    t = RollingWindow(db, schema, table)
    if not t.is_managed:
        l.error('%s.%s is not managed. Stopping.', schema, table)
        return
    for eligible_partition in dumper.eligible_to_dump(t):
        dump_file = dumper.dump_table(t, eligible_partition.name, eligible_partition.schema_only)
        l.info('%s.%s dumped to %s.', schema, table, dump_file)

##########################################################################
def dump(options):
    l = getLogger('dump')
    dumper = PartitionDumper(options)
    if options.table is not None:   # I'm rolling a single table
        dump_table(options.db, options.schema, options.table, dumper)
    else:
        l.debug('No table specified. Dump them all.')
        m = MaintainedTables(options.db)
        for managed_table in m:
            dump_table(options.db, managed_table[0], managed_table[1], dumper)

##########################################################################
def restore_table(r, dumper):
    l = getLogger('restore_table')
    l.debug('Restoring %s.%s', r.schema, r.table)
    for eligible_file in dumper.files_eligible_to_restore(r):
        l.info('%s.%s restoring file %s', r.schema, r.table, eligible_file)
        dumper.restore_file(r, eligible_file)

##########################################################################
def restore(options):
    l = getLogger('restore')
    dumper = PartitionDumper(options)
    if options.table is not None:   # I'm rolling a single table
        r = RollingWindow(options.db, options.schema, options.table)
        restore_table(r, dumper)
    else:
        l.debug('No table specified. Restore all locally maintained tables.')
        for r in dumper.tables_eligible_to_restore():
            restore_table(r, dumper)

##########################################################################
def cleanup(options):
    l = getLogger('cleanup')
    raise NotImplementedError('WRITEME')

##########################################################################
def repeat(action, options):
    """
    Given an action and an options block, run that action, with associated options
    approximately ever_n_seconds.
    """

    l = getLogger('repeat')
    run_interval = timedelta(seconds = float(options.every_n_seconds))
    l.debug('run_interval: %r', run_interval)
    last_completion_time = datetime.utcnow()
    while True:
        l.info('Running %s', action.__name__)
        action(options)
        # TODO: refactor this for easier UT
        this_completion_time = datetime.utcnow()
        run_time = this_completion_time - last_completion_time
        last_completion_time = this_completion_time
        l.info('Completed %s, took %s', action.__name__, run_time)
        if run_time < run_interval:
            wait_time = run_interval - run_time
            l.info('Waiting %s after running %s', wait_time, action.__name__)
            sleep(wait_time.seconds)
        else:
            l.debug('runtime > run_interval, so do not sleep')

##########################################################################
def wrap_post_execute(verb, post_execute):
    l = getLogger('post_execute')
    command = shlex_split(post_execute)
    @wraps(verb)
    def wrapped_verb(*args, **kwargs):
        try:
            verb(*args, **kwargs)
        finally:
            l.debug('Executing %r', command)
            check_call(command)
    return wrapped_verb

##########################################################################
def wrap_pre_execute(verb, pre_execute):
    l = getLogger('pre_execute')
    command = shlex_split(pre_execute)
    @wraps(verb)
    def wrapped_verb(*args, **kwargs):
        l.debug('Executing %r', command)
        check_call(command)
        verb(*args, **kwargs)
    return wrapped_verb

##########################################################################
# Interactive commands
actions = {
    'init': init,
    'add': add,
    'roll': roll,
    'list': list,
    'freeze': freeze,
    'dump': dump,
    'restore': restore,
    'cleanup': cleanup,
}

def main():
    usage="""usage: %prog <verb> <options>
List tables under management (or details about a specific table with the table parameter):
    list [[-n <schema>] -t <table>] [<PostgreSQL options>]

Adds table to the rolling window system for maintenance.
    add [-n <schema>] -t <table> -c <column> -s <step> -r <retention> -a <advanced> [<PostgreSQL options>]

Roll the table (or all maintained tables if no table parameter):
Specifying the --vacuum_parent_after_every may help for initial rolls on systems with low disk.
    roll [[-n <schema>] -t <table>] [--vacuum_parent_after_every 10] [<PostgreSQL options>]

Specify a column and optional lower_bound_overlap to configure a column for freezing.
If the lower_bound_overlap is specified, then the optional prior_upper_bound_percentile may also be specified.
The presence of the column paramater causes configuration behavior.
    freeze [[-n <schema>] -t <table> [-c <column> [--overlap <overlap> [--prior_upper_bound_percentile <percentile>]]]] [<PostgreSQL options>]
Otherwise, this will apply bound constraints to all configured columns in all eligible partitions for the table (or all maintained tables if no table parameter).
Specifying the --cluster parameter will cause the table to be clustered after freezing.
    freeze [[-n <schema>] -t <table>] [--cluster] [--freeze_after_cluster] [<PostgreSQL options>]
Since freeze needs to take an AccessExclusive lock, you may need to wrap this with pre and post execute parameters
    freeze ... --pre_execute "kill heavy process" --post_execute "restart heavy process"

Cleanup and (re-)freeze all eligible partitions for the table (or all maintained tables if no table parameter).
If a freeze_column and lower_bound_overlap are provided, apply that offset to the table.freeze_column,
moving data that is out of bounds into the limbo table. NOT IMPLEMENTED (yet)
    cleanup [[-n <schema>] -t <table> [-f <freeze_column> [--overlap <overlap>]]] [<PostgreSQL options>]

Dump all frozen / freezable partitions which have not yet been dumped:
    dump --dump_directory=/path/to/dir

Load all partition dump files that have not yet been loaded from the dump_directory:
NOTE: to remain consistent with pg_restore, if you do not provide a --dbname parameter,
this will stream the SQL to STDOUT.
    restore --dump_directory=/path/to/dir

Initialize the database with the rolling_window schema and internal database API:
NOTE: for this to work, you have to be running it in the same directory as the pg_rollingwindow_api.sql file.
    init [<PostgreSQL options>]

Note that for PostgreSQL options, standard libpq conventions are followed.
See http://www.postgresql.org/docs/current/static/libpq-envars.html')
"""

    parser = OptionParser(usage=usage, version=__vcs_id__, conflict_handler="resolve")
    log_group = OptionGroup(parser, 'Logging options',
        description='Default log level is WARNING. -q and -v are cumulative and reverse each other. -vvq would be the same as -v, which would be INFO level.')
    log_group.add_option('-q', '--quiet', dest='quiet_count', action='count')
    log_group.add_option('-v', '--verbose', dest='verbose_count', action='count')
    log_group.add_option('--logfile', default=None,
        help='if defined, instead of logging to stderr, append to this file')
    parser.add_option_group(log_group)

    parser.add_option('-t', '--table',
        help='required for add: act on this particular table')
    parser.add_option('-n', '--schema', default='public',
        help='... in this particular schema, defaulting to public')
    parser.add_option('-c', '--column',
        help='column to use in operation. For add, this is the partition key. For freeze, this is the freeze column.')
    parser.add_option('-s', '--step',
        help='partition width in terms of the partition column (lower_bound + step - 1 = upper_bound)')
    parser.add_option('-r', '--partition_retention', dest='partition_retention',
        help='target number of non-empty partitions to keep around, the width of the window')
    parser.add_option('-a', '--partition_ahead',
        help='target number of empty reserve partitions to keep ahead of the window')
    parser.add_option('--data_lag_window', default=0,
        help='partitions following the highest partition with data to hold back from freezing / dumping')
    parser.add_option('--cluster', action='store_true', default=False,
        help='CLUSTER partitions when freezing them')
    parser.add_option('--freeze_after_cluster', action='store_true', default=False,
        help='VACUUM FREEZE partitions after CLUSTERing them when freezing')
    parser.add_option('--overlap',
        help='what to subtract from the upper bound of the previous partition to generate the new lower bound for this partition for the associated column')
    parser.add_option('--prior_upper_bound_percentile',
        help='if set, determine the upper bound of the previous partition based on the top n\'th percentile of the max of that column, rather than simply grabbing it from the actual bound declaration. WARNING: this will fail when trying to freeze tables with very small data-sets.')
    parser.add_option('--vacuum_parent_after_every', default=0,
        help='when rolling data to partitions, VACUUM FULL the parent table after moving every n partitions. Super slow, but reclaims disk space. Not particularly useful except when rolling a lot of data out of the parent table.')
    parser.add_option('--pg_path',
        help='path for pg_dump and psql, default searchs system path')
    parser.add_option('--dump_directory',
        help='directory where dumps of partitions will dropped / searched for when using dump or undump command')
    parser.add_option('--every_n_seconds',
        help='if specified, repeat the action until killed')
    parser.add_option('--pre_execute',
        help='if specified, a shell command to run before any verb. If used in conjunction with the every_n_seconds, this command will be run repeatedly for every invocation of the verb. If this command fails, the verb will not be executed. Neither will post-execute.')
    parser.add_option('--post_execute',
        help='if specified, a shell command to run after any verb. If used in conjunction with the every_n_seconds, this command will be run repeatedly for every invocation of the verb. If verb fails, this command will still be run. If pre-execute is specified and fails, this will not be run.')

    postgres_group = OptionGroup(parser, 'PostgreSQL connection options', description='pg_rollingwindow makes an effort to support the same command line parameters and ENVIRONMENT variables as all other libpq based tools.')
    postgres_group.add_option('-h', '--host',
        help='Specifies the host name of the machine on which the server is running. If the value begins with a slash, it is used as the directory for the Unix-domain socket.')
    postgres_group.add_option('-d', '--dbname', dest='database',
        help='Specifies the name of the database to connect to.')
    postgres_group.add_option('-p', '--port',
        help='Specifies the TCP port or the local Unix-domain socket file extension on which the server is listening for connections. Defaults to the value of the PGPORT environment variable or, if not set, to the port specified at compile time, usually 5432.')
    postgres_group.add_option('-U', '--username',
        help='Connect to the database as the user username instead of the default. (You must have permission to do so, of course.)')
    postgres_group.add_option('-w', '--no-password', dest='password_prompt', action='store_false',
        help='Never issue a password prompt. IMPLICIT (use .pgpass or pg_hba).')
    postgres_group.add_option('-W', '--password', dest='password_prompt', action='store_true',
        help='Force prompt for a password. NOT SUPPORTED.')
    parser.add_option_group(postgres_group)

    (options, args) = parser.parse_args()
    l = getLogger()
    from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, StreamHandler, FileHandler, Formatter
    handler = StreamHandler() if options.logfile is None else FileHandler(options.logfile)
    formatter = Formatter('%(asctime)s %(name)s %(levelname)s %(filename)s:%(lineno)d: %(message)s')
    handler.setFormatter(formatter)
    l.addHandler(handler)
    raw_log_level = 2   # default to warn level
    if options.verbose_count is not None: raw_log_level += options.verbose_count
    if options.quiet_count is not None:   raw_log_level -= options.quiet_count
    options.ensure_value('verbosity', raw_log_level)
    if   raw_log_level <= 0: l.setLevel(CRITICAL)
    elif raw_log_level == 1: l.setLevel(ERROR)
    elif raw_log_level == 2: l.setLevel(WARNING)    # default
    elif raw_log_level == 3: l.setLevel(INFO)
    else:                    l.setLevel(DEBUG)

    action = actions.get(args.pop(0), None)
    if action is None:
        return parser.print_help()

    if options.post_execute is not None:
        action = wrap_post_execute(action, options.post_execute)

    if options.pre_execute is not None:
        action = wrap_pre_execute(action, options.pre_execute)

    options.ensure_value('db', PgConnection(options))
    if options.every_n_seconds is None:
        return action(options)
    else:
        repeat(action, options)

if __name__ == '__main__':
    main()
