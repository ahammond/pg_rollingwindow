#!/usr/bin/env python
from optparse import OptionGroup

__author__ = 'ahammond'
__copyright__ = 'Copyright (c) 2011 SmartReceipt assigned in perpetuity to PostgreSQL Global Development Group'
__license__ = 'PostgreSQL License, see http://www.postgresql.org/about/license'
__vcs_id__ = '$Id: $'

from logging import getLogger

class RollingTable(object):
    def __init__(self):
        l = getLogger('RollingTable.init')
        pass

    def implement_partition(self):
        pass

    def roll(self):
        pass


class MaintainedTables(object):
    def __init__(self):
        pass


def add(options):
    pass

def roll(options):
    pass

act = {
    'add': add,
    'roll': roll
}

def main():
    usage='''usage: %prog [add|roll] ...
Roll the table (or all maintained tables if no parameter):
    roll [-t <table>]

Adds table to the rolling window system for maintenance.
    add -t <table> -c <partition_column> -s <step> -r <retention>
    '''

    from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, StreamHandler
    from optparse import OptionParser, OptionGroup
    parser = OptionParser(usage=usage, version=__vcs_id__, conflict_handler="resolve")
    parser.add_option('-q', '--quiet', dest='quiet_count', action='count')
    parser.add_option('-v', '--verbose', dest='verbose_count', action='count')

    parser.add_option('-t', '--table', dest='parent',
        help='act on this particular table, required for add')
    parser.add_option('-c', '--partition_column', dest='partition_column',
        help='column to use as a partition key')
    parser.add_option('-s', '--partition_step', dest='partition_step',
        help='partition width in terms of the partition column (lower_bound + step - 1 = upper_bound)')
    parser.add_option('-r', '--partition_retention', dest='partition_retention',
        help='target number of partitions to keep around, the width of the window')

    postgres_group = OptionGroup(parser, 'PostgreSQL connection options')
    postgres_group.add_option('-h', '--host', dest='db_host',
        help='Specifies the host name of the machine on which the server is running. If the value begins with a slash, it is used as the directory for the Unix-domain socket.')
    postgres_group.add_option('-d', '--dbname', dest='db_name',
        help='Specifies the name of the database to connect to.')
    postgres_group.add_option('-p', '--port', dest='db_port',
        help='''Specifies the TCP port or the local Unix-domain socket file extension on which the server is listening for connections. Defaults to the value of the PGPORT environment variable or, if not set, to the port specified at compile time, usually 5432.''')
    postgres_group.add_option('-U', '--username', dest='db_username',
        help='Connect to the database as the user username instead of the default. (You must have permission to do so, of course.)')
    postgres_group.add_option('-w', '--no-password', dest='password_prompt', action='store_false',
        help='Never issue a password prompt.')
    postgres_group.add_option('-W', '--password', dest='password_prompt', action='store_true',
        help='Force prompt for a password.')
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

    return getattr(act, args[0], parser.print_help)(options)

if __name__ == '__main__':
    main()
