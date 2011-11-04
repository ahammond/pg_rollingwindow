-- Some commands for testing the sql api.

-- ./pg_rollingwindow.py init
-- ./pg_rollingwindow.py add -t foo -c name -s 10 -a 3 -r 999999999 -l 3
-- ./pg_rollingwindow.py freeze -t foo -c rnd
-- ./pg_rollingwindow.py freeze -t foo -c ts --overlap "'1 day'::interval"

-- TODO: write pgTAP tests.

SET search_path TO public ;

CREATE TABLE foo(
    id serial primary key,
    name bigint not null,
    ts timestamptz not null default now(),
    rnd bigint not null default (floor(random() * 9223372036854775807)),
    unique(name, ts)
) ;
CREATE INDEX foo_rnd ON foo(rnd) WHERE rnd < 10000;

INSERT INTO foo(name) SELECT a FROM generate_series(1, 99) AS s(a) ;
INSERT INTO foo(name) SELECT a FROM generate_series(1, 99) AS s(a) ;
INSERT INTO foo(name) SELECT a FROM generate_series(1, 99) AS s(a) ;

CREATE SCHEMA bar ;
SET search_path TO bar ;

CREATE TABLE baz (
    id serial primary key,
    name bigint not null,
    ts timestamptz not null default now(),
    rnd bigint not null default (floor(random() * 9223372036854775807)),
    unique(name, ts)
) ;

INSERT INTO baz(name) SELECT a FROM generate_series(1, 99) AS s(a) ;
INSERT INTO baz(name) SELECT a FROM generate_series(1, 99) AS s(a) ;
INSERT INTO baz(name) SELECT a FROM generate_series(1, 99) AS s(a) ;

DROP SCHEMA rollingwindow CASCADE ;

\i ./pg_rollingwindow_api.sql

SET search_path TO public ;

-- INSERT INTO rolling_window.maintained_table(relid, attname, step, non_empty_partitions_to_keep, reserve_partitions_to_keep)
-- SELECT c.oid AS relid, 'name' AS attname, 10 AS step, 10 AS non_empty_partitions_to_keep, 10 AS reserve_partitions_to_keep
-- FROM pg_catalog.pg_class c
-- INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
-- WHERE n.nspname = 'public'
--   AND c.relname = 'foo'
-- ;
--
-- INSERT INTO rolling_window.columns_to_freeze(relid, column_name)
-- SELECT m.relid, 'ts' AS column_name
-- FROM rolling_window.maintained_table m
-- ;

SELECT rolling_window.lower_bound_from_child_name('child1_name_000001234'::name) ;

SELECT rolling_window.child_name('parent_name', 123) ;

SELECT * FROM rolling_window.min_max_in_parent_only('public', 'foo') ;

SELECT p FROM rolling_window.add_partitions_for_data_range('public', 'foo', NULL, NULL) AS a(p) ;

-- clone_indexes, but not to_limbo
SELECT * FROM rolling_window.move_data_to_partition('public', 'foo', 10, True, False);

-- clone_indexes and to_limbo
SELECT * FROM rolling_window.move_data_to_partition('public', 'foo', 20, True, True);

-- Used by move_lowest_data_to_partition when 3rd parameter is True
SELECT * FROM rolling_window.clone_indexes_to_partition('public', 'foo', 0) ;

SELECT move_lowest_data_to_partition('public', 'foo', True) ;

SELECT * FROM rolling_window.extend_table_reserve_partitions('public', 'foo') ;

SELECT * FROM rolling_window.trim_expired_table_partitions('public', 'foo') ;

SELECT * FROM rolling_window.list_partitions('public', 'foo') ;

SELECT rolling_window.constrain_partition('public', 'foo', 10, 'ts') ;

SELECT * FROM rolling_window.freeze_partition('public', 'foo', 0) ;

SELECT rolling_window.highest_freezable('public', 'foo') ;

SELECT * FROM rolling_window.freeze('public', 'foo') ;

