pg_rollingwindow

This is a tool to maintain rolling window tables in PostgreSQL.

A rolling window table is a table with the following characteristics
1) Has a FIFO retention policy (first in, first out)
2) Has a bigint not null column that is (generally) increasing over time,
which can be used for partitioning.
TODO: support timestamps for the partitioning column? extract(epoch from ...)
3) Retention requirements can be defined by a range of values in the partition column.
In other words, keep everything where partition_column is greater than x and x is
a value that increases over time.

Note, this partition column does not need to be unique.
It could be decreasing rather than increasing, as long as it's consistent.
However, code to support decreasing partition column values has yet to be written.
Neither does it need to have strong ordering for inserts,
although be aware that inserts of "old" entries can junk up the parent table,
if the partition for that old entry has already been dropped.

Freezing columns will place constraints on older partition tables that reflect
their contents, allowing PostgreSQL's constraint based exclusion to speed up
your queries. Once a partition has been frozen, you may not be able to add
further data to it (stuff that conflicts with the constraints obviously won't
be allowed in)... so don't freeze if you expect substantial out of order input.
Such data will end up in the limbo table.
Data in limbo is bad for your performance as it can never be CBE excluded.
It's probably a good idea to go clean it up on a regular basis.

NOTE: The index cloning code is not currently all that smart.
It will fail on add while attempting to create the limbo partition (or clone indexes to partitions)
in cases where either the index name, the table name or the column name involved contain
any of the following strings: " USING " or " ON ". Note the leading and following spaces.
Solving this would probably involve clever tricks with regex.
Probably not impossible, but certainly not trivial and more work than such a tiny corner merits.

Cleaning up data in limbo can be done a bunch of ways. You could just delete it.
Or, you could delete stuff older than the oldest partition (this seems reasonable).
Then for stuff that should belong in other partitions, you can move it there manually.

REQUIREMENTS
PostgreSQL 9.0 (probably works with older versions, but not tested)
Python 2.6 (possibly works with 2.5 or even 2.4, but... not tested)
 - psycopg2
Python unit testing (developers only)
 - Mock (http://www.voidspace.org.uk/python/mock/)

INSTALLATION

export PGHOST=localhost
export PGDATABASE=testdb

From your checkout directory,

pg_rollingwindow.py init

Then for tables that you want to manage,
pg_rollingwindow.py add -t tbl -c id -s 10000 -r 5000 -a 300 -

And for columns you'd like to freeze within those tables
pg_rollingwindow.py freeze -t tbl -c rnd
pg_rollingwindow.py freeze -t tbl -c tstmp --overlap "'2 days'::interval"

BASIC USAGE
Create a cron job that regularly runs

pg_rollingwindow.py roll

And, if you have specified any freeze columns,

pg_rollingwindow.py freeze

TODO
Ideas for next steps / features are currently inline in the code.
