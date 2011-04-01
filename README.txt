pg_rollingwindow

This is a tool to maintain rolling window tables in PostgreSQL.

A rolling window table is a table with the following characteristics
1) Has a FIFO retention policy (first in, first out)
2) Has a bigint not null column that is (generally) increasing over time, which can be used for partitioning.
3) Retention requirements can be defined by a range of values in the partition column.
(in other words, keep data where partition_column BETWEEN lower_bound AND upper_bound)

Note, this partition column does not need to be unique.
It could be decreasing rather than increasing, as long as it's consistent.
Code to support decreasing has yet to be written.
Neither does it need to have strong ordering for inserts,
although be aware that inserts of "old" entries can junk up the parent table,
if the partition for that old entry has already been dropped.

Freezing columns will place constraints on older partition tables that reflect
their contents, allowing PostgreSQL's constraint based exclusion to speed up
your queries. Once a partition has been frozen, you may not be able to add
further data to it (stuff that conflicts with the constraints obviously won't
be allowed in)... so don't freeze if you expect substantial out of order input.
Such data will end up in the limbo table. Data in limbo is bad for you, go clean it up.

Cleaning up data in limbo can be done a bunch of ways. You could just delete it.
Or, you could delete stuff older than the oldest partition (this seems reasonable).
Then for stuff that should belong in other partitions, you can

REQUIREMENTS
PostgreSQL 9.0 (possibly works with older versions, but not tested)
Python 2.6 (possibly works with 2.5, but... not tested)
 - psycopg2

INSTALLATION
psql -f pg_rollingwindow_api.sql
# This will create the rolling_window schema in your database.

pg_rollingwindow.py add -t table -c transaction_id -s 10000 -r 5000 -a 300 -f tstamp
pg_rollingwindow.py roll

BASIC USAGE
Create a cron job that regularly runs

pg_rollingwindow.py roll

And, if it fits your data's behavior,

pg_rollingwindow.py freeze

TODO
Ideas for next steps / features are currently inline in the code.
