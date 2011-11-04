-- do not allow inserts into parent tables.

CREATE SCHEMA rolling_window;
SET search_path TO rolling_window,public;

CREATE TABLE maintained_table (
    relid oid PRIMARY KEY, -- Would like to add: REFERENCES pg_catalog.pg_class (oid) ON DELETE CASCADE ON UPDATE RESTRICT,
    attname name NOT NULL,
    step bigint NOT NULL,
    non_empty_partitions_to_keep bigint NOT NULL,   -- partitions should generally be filled in order
    reserve_partitions_to_keep bigint NOT NULL,
    data_lag_window bigint NOT NULL DEFAULT(0),     -- This is a kind of risky default
    last_partition_dumped bigint NOT NULL DEFAULT(-1),
    partitioned_on TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT transaction_timestamp(),
    rolled_on TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT transaction_timestamp()
    -- Would like to add: FOREIGN KEY (attid, relid) REFERENCES pg_catalog.pg_attribute (oid, attrelid) MATCH FULL ON DELETE CASCADE ON UPDATE RESTRICT
);
COMMENT ON TABLE maintained_table
IS 'Store metadata for rolling_window. Each table under management has an entry here.';
COMMENT ON COLUMN maintained_table.relid
IS 'pg_class.oid reference to the table being managed';
COMMENT ON COLUMN maintained_table.attname
IS 'pg_attribute.attname refrence of the partition key column';
COMMENT ON COLUMN maintained_table.step
IS 'size of increment between partition tables';
COMMENT ON COLUMN maintained_table.non_empty_partitions_to_keep
IS 'drop oldest partitions when we have more than this many partition child tables';
COMMENT ON COLUMN maintained_table.reserve_partitions_to_keep
IS 'how many partitions should we maintain in front of the one receiving data';
COMMENT ON COLUMN maintained_table.last_partition_dumped
IS 'lower bound of the highest partition which has been dumped';
COMMENT ON COLUMN maintained_table.data_lag_window
IS 'how many partitions past the first non-empty partition to hold open for late arriving data (hold off from freezing / dumping these partitions)';
COMMENT ON COLUMN maintained_table.partitioned_on
IS 'when we first partitioned this table';
COMMENT ON COLUMN maintained_table.rolled_on
IS 'when were we last told to maintain this table';



---------------------------------------------------------------------
CREATE TABLE columns_to_freeze (
    relid oid REFERENCES maintained_table(relid) ON DELETE CASCADE,
    column_name name,
    lower_bound_overlap text,
    PRIMARY KEY (relid, column_name)
);
COMMENT ON TABLE columns_to_freeze
IS 'A list of columns which may be frozen with "bound" constraints.';
COMMENT ON COLUMN columns_to_freeze.relid
IS 'The pg_class.oid of the table involved.';
COMMENT ON COLUMN columns_to_freeze.column_name
IS 'The pg_attribute.attname of the column to be frozen.';
COMMENT ON COLUMN columns_to_freeze.lower_bound_overlap
IS 'when not NULL, what to subtract from the upper bound of the previous partition to generate the lower bound for this column when freezing.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lower_bound_from_child_name(
    child name
) RETURNS bigint AS $definition$
SELECT CAST(regexp_replace($1, E'.*_(\\d+)$', E'\\1') AS bigint) AS lower_bound
$definition$ LANGUAGE sql;
COMMENT ON FUNCTION lower_bound_from_child_name(name)
IS 'Given the name of the parent table and the lower_boundary of the partition, return the name of the associated child partition table.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION child_name(
    parent name,
    lower_bound bigint
) RETURNS name AS $definition$
-- Zero pad: bigint_max is 9223372036854775807. floor(log(bigint_max)) is 18
SELECT CAST(
    $1 || '_'
    || repeat('0', (18 - CASE WHEN 0 = $2 THEN 0 ELSE floor(log($2)) END)::int)
    || $2 AS name
) AS child
$definition$ LANGUAGE sql;
COMMENT ON FUNCTION child_name(name, bigint)
IS 'Given the name of the parent table and the lower_boundary of the partition, return the name of the associated child partition table.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION min_max_in_parent_only(
    parent_namespace name,
    parent name,
    OUT min_value bigint,
    OUT max_value bigint,
    OUT step bigint
) RETURNS RECORD AS $definition$
DECLARE
    attname name;
    select_str text;
BEGIN
    SELECT m.attname, m.step
        INTO attname, step
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
          AND n.nspname = parent_namespace;
    IF attname IS NULL
    THEN
        RAISE EXCEPTION 'table not found in rolling_window.maintained_table';
    END IF;
    select_str := 'SELECT min(' || quote_ident(attname)
        || ') , max(' || quote_ident(attname)
        || ') FROM ONLY ' || quote_ident(parent_namespace)
        || '.' || quote_ident(parent);
    EXECUTE select_str INTO min_value, max_value;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION min_max_in_parent_only(name, name, OUT bigint, OUT bigint, OUT bigint)
IS 'Find the minimum and maximum values of data that is in the parent table (and only the parent table). Grab step while we are at it.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION add_partition(
    parent_namespace name,
    parent name,
    lower_bound bigint
) RETURNS name AS $definition$
DECLARE
    child name;
    attname name;
    step bigint;
    upper_bound bigint;
    create_str text;
BEGIN
    SELECT m.attname, m.step
        INTO attname, step
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
          AND n.nspname = parent_namespace;
    IF step IS NULL
    THEN
        RAISE EXCEPTION 'table not found in rolling_window.maintained_table';
    END IF;
    child := rolling_window.child_name(parent, lower_bound);
    upper_bound := lower_bound + step - 1;
    create_str := 'CREATE TABLE ' || quote_ident(parent_namespace) || '.' || quote_ident(child)
        || ' ( CHECK ( '|| quote_ident(attname)|| ' BETWEEN '|| lower_bound || ' AND '|| upper_bound || ' ) ) '
        || 'INHERITS ( ' || quote_ident(parent_namespace) || '.' || quote_ident(parent) || ' )';
    EXECUTE create_str;
    RETURN child;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION add_partition(name, name, bigint)
IS 'Create a partition on parent, starting from lower_bound. Does not clone indexes. Use clone_indexes_to_partition(parent, lower_bound) to clone indexes. Refers to rolling_window.maintained_table for step and attname.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION add_limbo_partition(
    parent_namespace name,
    parent name
) RETURNS name AS $definition$
DECLARE
    child name;
    create_str text;
    index_oid oid;
    index_query_str text := $q$
        SELECT indexrelid FROM pg_index
        WHERE indrelid = (
            SELECT c.oid FROM pg_catalog.pg_class c
            INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
            WHERE nspname = $1
              AND relname = $2
              AND relkind = 'r')
        $q$;
    parent_index_str text;
    where_start int;
    where_str text;
    tablespace_start int;
    tablespace_str text;
    with_start int;
    with_str text;
    using_start int;
    using_str text;
    on_start int;
    on_str text;
    index_name_start int;
    index_name_str text;
    new_index_name_str text;
    create_index_str text;
BEGIN
    child := parent || '_limbo';
    IF EXISTS ( SELECT 1
        FROM pg_catalog.pg_class c
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE relname = child
          AND nspname = parent_namespace )
    THEN
        RETURN NULL;    -- already exists.
    END IF;
    create_str := 'CREATE TABLE ' || quote_ident(child)
        || '() INHERITS ( ' || quote_ident(parent) || ' )';
    EXECUTE create_str;

    PERFORM * FROM rolling_window.clone_indexes_to_partition(parent_namespace, parent, child);
    RETURN child;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION add_limbo_partition(name, name)
IS 'Create a limbo partition (with cloned indexes) on parent. This is where data that can not be moved into a frozen or already trimmed partition ends up.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION add_partitions(
    parent_namespace name,
    parent name,
    start bigint,
    stop bigint
) RETURNS SETOF name AS $definition$
DECLARE
    step bigint;
BEGIN
    SELECT m.step
        INTO step
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
          AND n.nspname = parent_namespace;
    IF step IS NULL
    THEN
        RAISE EXCEPTION 'table not found in rolling_window.maintained_table';
    END IF;
    FOR lower_bound IN start..stop BY step
    LOOP
        RETURN NEXT rolling_window.add_partition(parent_namespace, parent, lower_bound);
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION add_partitions(name, name, bigint, bigint)
IS 'Create a range of partitions.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION add_partitions_for_data_range(
    parent_namespace name,
    parent name,
    manual_min bigint,
    manual_max bigint
) RETURNS SETOF name AS $definition$
DECLARE
    step bigint;
    attname name;
    manual_min_str text;
    manual_max_str text;
    min_value bigint;
    max_value bigint;
    start_bound bigint;
    stop_bound bigint;
    select_bounds_str text;
    child_name name;
BEGIN
    SELECT m.step, m.attname
        INTO step, attname
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
          AND n.nspname = parent_namespace;
    IF step IS NULL
    THEN
        RAISE EXCEPTION 'table not found in rolling_window.maintained_table';
    END IF;
    IF manual_min IS NOT NULL AND manual_max IS NOT NULL
    THEN
        min_value := manual_min;
        max_value := manual_max;
    ELSE
        manual_min_str := CAST(manual_min AS text);
        manual_max_str := CAST(manual_max AS text);
        select_bounds_str := 'SELECT COALESCE(' || quote_nullable(manual_min_str) || ', min(' || quote_ident(attname) || ')), '
            || 'COALESCE(' || quote_nullable(manual_max_str) || ', max(' || quote_ident(attname)
            || ')) FROM ONLY ' || quote_ident(parent_namespace) || '.' || quote_ident(parent);
        EXECUTE select_bounds_str INTO min_value, max_value;
        IF min_value IS NULL OR max_value IS NULL
        THEN
            min_value := -1;
            max_value := -1;
        END IF;
    END IF;
    start_bound := min_value - min_value % step;
    stop_bound := max_value - max_value % step;
    FOR lower_bound IN start_bound..stop_bound BY step
    LOOP
        -- does the partition already exist?
        child_name := rolling_window.child_name(parent, lower_bound);
        IF child_name NOT IN (SELECT p.relname FROM rolling_window.list_partitions(parent_namespace, parent) AS p)
        THEN
            RETURN NEXT rolling_window.add_partition(parent_namespace, parent, lower_bound);
        END IF;
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION add_partitions_for_data_range(name, name, bigint, bigint)
IS 'Create partitions covering the entire range of data in the table. If either bound is NULL then the maximum or minimum value in the table will be used.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION clone_indexes_to_partition(
    parent_namespace name,
    parent name,
    lower_bound bigint
) RETURNS SETOF TEXT AS $definition$
DECLARE
    child name;
    index_str text;
BEGIN
    child := rolling_window.child_name(parent, lower_bound);
    FOR index_str IN
        SELECT r.a
        FROM rolling_window.clone_indexes_to_partition(parent_namespace, parent, child) AS r(a)
    LOOP
        RETURN NEXT index_str;
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION clone_indexes_to_partition(name, name, bigint)
IS 'Apply all the indexes on a parent table to a partition.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION clone_indexes_to_partition(
    parent_namespace name,
    parent name,
    child name
) RETURNS SETOF TEXT AS $definition$
DECLARE
    index_query_str text := $q$
        SELECT pg_get_indexdef(indexrelid)
        FROM pg_index
        WHERE indrelid = (
            SELECT c.oid FROM pg_catalog.pg_class c
            INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
            WHERE nspname = $1
              AND relname = $2
              AND relkind = 'r')
        $q$;
    parent_index_str text;
    where_start int;
    where_str text;
    tablespace_start int;
    tablespace_str text;
    with_start int;
    with_str text;
    using_start int;
    using_str text;
    on_start int;
    on_str text;
    index_name_start int;
    index_name_str text;
    new_index_name_str text;
    create_index_str text;
    parent_name_position int;
BEGIN
    FOR parent_index_str IN EXECUTE index_query_str USING parent_namespace, parent
    LOOP
        -- parse the index create string starting from the end and going towards the front
        where_start := position(' WHERE ' IN parent_index_str);
        IF where_start > 0
        THEN
            where_str := substring(parent_index_str FROM where_start);
            parent_index_str := substring(parent_index_str FROM 1 FOR where_start - 1);
        ELSE
            where_str := '';
        END IF;
        tablespace_start := position(' TABLESPACE ' in parent_index_str);
        IF tablespace_start > 0
        THEN
            tablespace_str := substring(parent_index_str FROM tablespace_start);
            parent_index_str := substring(parent_index_str FROM 1 FOR tablespace_start - 1);
        ELSE
            tablespace_str := '';
        END IF;
        with_start := position(' WITH ' IN parent_index_str) + length('');
        IF with_start > 0
        THEN
            -- WRITEME: handle cases where we already have WITH() stuff
            RAISE 'pg_rollingwindow does not yet handle indexes that already have WITH clauses';
        ELSE
            with_str := ' WITH (fillfactor = 100)';
        END IF;
        using_start := position(' USING ' IN parent_index_str);
        using_str := substring(parent_index_str FROM using_start);
        parent_index_str := substring(parent_index_str FROM 1 FOR using_start - 1);

        on_start := position(' ON ' IN parent_index_str);
        on_str := substring(parent_index_str FROM on_start);
        parent_index_str := substring(parent_index_str FROM 1 FOR on_start - 1);

        index_name_start := position(' INDEX ' IN parent_index_str) + length(' INDEX ');
        index_name_str := substring(parent_index_str FROM index_name_start);

        parent_name_position := position(parent IN index_name_str) ;
        IF parent_name_position > 0   -- if the parent name is in the index name.
        THEN    -- We should replace the leftmost instance of the parent name with the child name inside the new index's name.
            new_index_name_str :=  substring(index_name_str from 1 for parent_name_position - 1)
                || child
                || substring(index_name_str from parent_name_position + length(parent));
        ELSE    -- We should just give it something reasonable for an index name, so stick the child's name on as a prefix.
            new_index_name_str := child || '_' || index_name_str;
        END IF;

        -- TODO: address cloning of UNIQUE / PRIMARY KEY constraints as actual constraints rather than just swooping their indexes.
        parent_index_str := substring(parent_index_str FROM 1 FOR index_name_start -1);

        create_index_str := parent_index_str
            || quote_ident(new_index_name_str)
            || ' ON ' || quote_ident(child)
            || using_str
            || with_str
            || tablespace_str
            || where_str;
        EXECUTE create_index_str;
        RETURN NEXT create_index_str;
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION clone_indexes_to_partition(name, name, name)
IS 'Apply all the indexes on a parent table to a partition. Rather than specifying a lower_bound, this requires the child name to support working with limbo tables.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION move_data_to_partition(
    parent_namespace name,
    parent name,
    lower_bound bigint,
    clone_indexes boolean,
    to_limbo boolean
) RETURNS bigint AS $definition$
DECLARE
    upper_bound bigint;
    child name;
    attname name;
    step bigint;
    insert_str text;
    insert_count bigint;
    delete_str text;
    delete_count bigint;
    index_str text;
    best_index_name name;
    index_name name;
    index_is_unique bigint;
    best_index_position bigint;
    index_position bigint;
    alter_str text;
BEGIN
    SELECT m.attname, m.step
        INTO attname, step
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
          AND n.nspname = parent_namespace;
    IF to_limbo
    THEN
        child := parent || '_limbo';
    ELSE
        child := rolling_window.child_name(parent, lower_bound);
    END IF;
    upper_bound = lower_bound + step - 1;
    insert_str := 'INSERT INTO ' || quote_ident(parent_namespace) || '.' || quote_ident(child)
        || ' SELECT * FROM ONLY ' || quote_ident(parent_namespace) || '.' || quote_ident(parent)
        || ' WHERE ' || quote_ident(attname) || ' BETWEEN $1 AND $2';
    EXECUTE insert_str USING lower_bound, upper_bound;
    GET DIAGNOSTICS insert_count = ROW_COUNT;
    delete_str := 'DELETE FROM ONLY ' || quote_ident(parent_namespace) || '.' || quote_ident(parent)
        || ' WHERE ' || quote_ident(attname)
        || ' IN ( SELECT ' || quote_ident(attname)
        || ' FROM ' || quote_ident(parent_namespace) || '.' || quote_ident(child)
        || ')';
    EXECUTE delete_str;
    GET DIAGNOSTICS delete_count = ROW_COUNT;
    IF insert_count != delete_count
    THEN
        RAISE EXCEPTION 'Inserted % rows, but attempted to delete % rows.', insert_count, delete_count;
    END IF;
    IF clone_indexes
    THEN
        FOR index_str IN
            SELECT r.a
            FROM rolling_window.clone_indexes_to_partition(parent_namespace, parent, child) AS r(a)
        LOOP
            index_str := substring(index_str from position(' INDEX ' in index_str) + length(' INDEX '));
            index_name := substring(index_str from 1 for position(' ON ' in index_str) - 1);
            -- Chomp off everything up to the column list.
            index_str := substring(index_str from position(' USING (' in index_str) + length(' USING ('));
            index_is_unique := position('UNIQUE' in index_str);
            index_position := position(attname IN index_str);

            -- we can't cluster on partial indexes
            CONTINUE WHEN position(' WHERE ' IN index_str) > 0;

            -- if we don't have an index, so take the first we get
            IF best_index_name IS NULL
            THEN
                best_index_name := index_name;
            END IF;

            -- unique indexes are the best possible
            IF 0 <= index_is_unique
            THEN
                best_index_name := index_name;
                EXIT;
            END IF;

            -- indexes which mention our partitioning column are better.
            IF 0 <= index_position
            THEN
                IF best_index_position IS NULL
                THEN
                    best_index_name := index_name;
                    best_index_position := index_position;
                ELSE
                    -- prefer indexes where the partitioning column comes first.
                    IF index_position < best_index_position
                    THEN
                        best_index_name := index_name;
                        best_index_position := index_position;
                    END IF;
                END IF;
            END IF;
        END LOOP;

        IF best_index_name IS NOT NULL
        THEN
            alter_str := 'ALTER TABLE ' || quote_ident(parent_namespace) || '.' || quote_ident(child)
                || ' CLUSTER ON ' || quote_ident(best_index_name);
            EXECUTE alter_str;
        END IF;
    END IF;
    RETURN delete_count;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION move_data_to_partition(name, name, bigint, boolean, boolean)
IS 'Given a table and a lower_bound, move data that belongs in a given partition from the parent to the appropriate partition. Optionally clone indexes. Optionally move the data to the limbo table instead.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION move_highest_data_to_partition(
    parent_namespace name,
    parent name,
    clone_indexes boolean
) RETURNS bigint AS $definition$
DECLARE
    attname name;
    step bigint;
    select_max_value_str text;
    max_value bigint;
    lower_bound bigint;
BEGIN
    SELECT m.attname, m.step
        INTO attname, step
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
          AND n.nspname = parent_namespace;
    select_max_value_str := 'SELECT max(' || quote_ident(attname)
        || ') FROM ONLY ' || quote_ident(parent_namespace) || '.' || quote_ident(parent);
    EXECUTE select_max_value_str INTO max_value;
    IF max_value IS NULL
    THEN
        RETURN 0;
    END IF;
    lower_bound := max_value - max_value % step;
    RETURN rolling_window.move_data_to_partition(parent_namespace, parent, lower_bound, clone_indexes, false);
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION move_highest_data_to_partition(name, name, boolean)
IS 'Move one partitions worth of data from the parent to a partition table. Move highest data. Returns number of rows moved. Optionally clones indexes when done moving. Clone indexes only after you expect no further data for a given partition to be inserted into the parent table.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION move_lowest_data_to_partition(
    parent_namespace name,
    parent name,
    clone_indexes boolean
) RETURNS bigint AS $definition$
DECLARE
    min_value bigint;
    lower_bound bigint;
    attname name;
    step bigint;
    select_min_value_str text;
BEGIN
    SELECT m.attname, m.step
        INTO attname, step
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
          AND n.nspname = parent_namespace;
    select_min_value_str := 'SELECT min(' || quote_ident(attname)
        || ') FROM ONLY ' || quote_ident(parent_namespace) || '.' || quote_ident(parent);
    EXECUTE select_min_value_str INTO min_value;
    IF min_value IS NULL
    THEN
        RETURN 0;
    END IF;
    lower_bound := min_value - min_value % step;
    RETURN rolling_window.move_data_to_partition(parent_namespace, parent, lower_bound, clone_indexes, false);
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION move_lowest_data_to_partition(name, name, boolean)
IS 'Move one partitions worth of data from the parent to a partition table. Move lowest data. Returns number of rows moved. COptionally clone indexes when done moving. Only clone indexes after you expect no further data for a given partition to be inserted into the parent table.';

---------------------------------------------------------------------
CREATE TABLE list_partitions_result (
    partition_table_oid oid,
    CONSTRAINT no_rows CHECK (partition_table_oid = 0)
) INHERITS (pg_catalog.pg_class);

-- TODO: if we can switch this to using the LIKE clause then it will
-- remove the requirement for the user to be a database superuser.
-- However, the whole point of using INHERITS is that it eliminates
-- any issues with pg catalog schema changes on upgrade.
-- Maybe this is a non-issue?
-- What if the procedure for database upgrades is to re-init / add?
-- I don't like that approach since it demands user interaction.


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION list_partitions(
    parent_namespace name,
    parent name
) RETURNS SETOF rolling_window.list_partitions_result AS $definition$
SELECT c.*,
       c.oid AS partition_table_oid
FROM pg_catalog.pg_class c
WHERE c.oid IN
    (
        SELECT i.inhrelid FROM pg_catalog.pg_inherits i
        WHERE i.inhparent =
            (
                SELECT pc.oid
                FROM pg_catalog.pg_class pc
                INNER JOIN pg_catalog.pg_namespace n ON (pc.relnamespace = n.oid)
                WHERE pc.relname = $2
                  AND n.nspname = $1
            )
    )
$definition$ LANGUAGE sql;
COMMENT ON FUNCTION list_partitions(name, name)
IS 'Return pg_catalog.pg_class entries for child tables.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION extend_table_reserve_partitions(
    parent_namespace name,
    parent name
) RETURNS SETOF name AS $definition$
DECLARE
    step bigint;
    reserve_partitions_to_keep bigint;
    reltuples real;
    highest_partition name;
    current_partition name;
    lower_bound bigint;
    current_reserve_partitions bigint := 0;
BEGIN
    SELECT m.step, m.reserve_partitions_to_keep
        INTO step, reserve_partitions_to_keep
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE n.nspname = parent_namespace
          AND c.relname = parent;
    IF reserve_partitions_to_keep IS NULL
    THEN
        RAISE EXCEPTION 'table not found in rolling_window.maintained_table';
    END IF;
    FOR current_partition, reltuples IN
        SELECT p.relname, p.reltuples
        FROM rolling_window.list_partitions(parent_namespace, parent) AS p
        WHERE p.relname ~ (parent || E'_\\d+$')
        ORDER BY p.relname DESC
    LOOP
        IF highest_partition IS NULL
    THEN
            highest_partition := current_partition;
        END IF;
        EXIT WHEN reltuples > 0;    -- reltuples = 0 means known empty
        current_reserve_partitions := current_reserve_partitions + 1;
    END LOOP;
    IF highest_partition IS NULL
    THEN
        RAISE EXCEPTION 'This table has not yet been partitioned. I am not yet smart enough to work with this kind of table.';
    END IF;
    lower_bound := rolling_window.lower_bound_from_child_name(highest_partition);
    FOR junk IN current_reserve_partitions .. reserve_partitions_to_keep
    LOOP
        lower_bound := lower_bound + step;
        RETURN NEXT rolling_window.add_partition(parent_namespace, parent, lower_bound);
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION extend_table_reserve_partitions(name, name)
IS 'Find the highest partition table with any data in it, then add reserve partitions ahead of it as necessary.';


---------------------------------------------------------------------
CREATE TYPE trim_result AS (
    partition_table_name name,
    reltuples real,
    total_relation_size_in_bytes bigint
);


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trim_expired_table_partitions(
    parent_namespace name,
    parent name
) RETURNS SETOF rolling_window.trim_result
AS $definition$
DECLARE
    partition_table_oid oid;
    partition_table_name name;
    non_empty_partitions_to_keep bigint;
    reltuples real;
    drop_str text;
    total_relation_size_in_bytes bigint;
    t_result rolling_window.trim_result;
BEGIN
    SELECT m.non_empty_partitions_to_keep
        INTO non_empty_partitions_to_keep
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
        AND n.nspname = parent_namespace;
    IF non_empty_partitions_to_keep IS NULL
    THEN
        RAISE EXCEPTION 'table not found in rolling_window.maintained_table';
    END IF;
    -- count down through the children to find partitions that exceed our retention policy
    FOR partition_table_oid, partition_table_name, reltuples IN
        SELECT p.partition_table_oid, p.relname, p.reltuples
        FROM rolling_window.list_partitions(parent_namespace, parent) AS p
        WHERE p.relname ~ (parent || E'_\\d+$')
        ORDER BY p.relname DESC
    LOOP
        IF non_empty_partitions_to_keep <= 0    -- we have run out of partitions to keep
        THEN
            total_relation_size_in_bytes := pg_total_relation_size(partition_table_oid);
            -- TODO: would it be better to first ALTER TABLE partition_table NO INHERIT then DROP?
            -- Probably not, unless we decouple the ALTER from the DROP transaction.
            drop_str := 'DROP TABLE ' || partition_table_name;
            EXECUTE drop_str;
            t_result := ROW(partition_table_name, reltuples, total_relation_size_in_bytes);
            RETURN NEXT t_result;
        ELSE
            IF reltuples > 0    -- reltuples = 0 means known empty
            THEN
                non_empty_partitions_to_keep := non_empty_partitions_to_keep - 1;
            END IF;
        END IF;
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION trim_expired_table_partitions(name, name)
IS 'Remove any partitions which extend beyond retention policy as defined by maintained_table.full_partitions_to_keep.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION move_data_below_lower_bound_overlap_to_limbo(
    parent_namespace name,
    parent name,
    lower_bound bigint
) RETURNS bigint AS $definition$
DECLARE
    parent_relid oid;
    step bigint;
    old_child_relid oid;
    constrained_column name;
    lower_bound_overlap text;
    where_clause text;
    old_bound_src text;
    ordinal text;
    old_upper_bound_start int;
    old_upper_bound_length int;
    old_upper_bound text;
    new_lower_bound text;
    new_type text;
    factor text;
    boundary_math text;
    child_name name;
    insert_str text;
    insert_count bigint;
    delete_str text;
    delete_count bigint;
BEGIN
    SELECT m.relid, m.step
        INTO parent_relid, step
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
        AND n.nspname = parent_namespace;
    IF step IS NULL
    THEN
        RAISE EXCEPTION 'table not found in rolling_window.maintained_table';
    END IF;

    IF lower_bound - step < 0
    THEN
        RETURN 0;
    END IF;

    SELECT p.partition_table_oid
        INTO old_child_relid
        FROM rolling_window.list_partitions(parent_namespace, parent) AS p
        WHERE p.relname = rolling_window.child_name(parent, lower_bound - step);
    IF old_child_relid IS NULL
    THEN    -- since there is no older child partition, we have nothing to move.
        RETURN 0;
    END IF;

    FOR constrained_column, lower_bound_overlap IN
        SELECT f.column_name, f.lower_bound_overlap
        FROM rolling_window.columns_to_freeze f
        WHERE f.relid = parent_relid
    LOOP
        --If we are not worried about overlap for this column...
        CONTINUE WHEN lower_bound_overlap IS NULL;
        old_bound_src := NULL;

        -- find the old_child's bound_foo constraint, if any
        SELECT c.consrc
            INTO old_bound_src
            FROM pg_catalog.pg_constraint c
            WHERE conname = 'bound_' || constrained_column
              AND conrelid = old_child_relid;
        CONTINUE WHEN old_bound_src IS NULL;      -- we don't have an older bound, so... next?

        -- parse out the upper bound
        ordinal := constrained_column || ' <= ';
        old_upper_bound_start := position(ordinal in old_bound_src) + length(ordinal);
        old_upper_bound_length := 1 + length(old_bound_src) - old_upper_bound_start - length('))');
        old_upper_bound := substring(old_bound_src from old_upper_bound_start for old_upper_bound_length);

        CONTINUE WHEN old_upper_bound_start <= 0;   -- this should never happen, but...

        -- calculate the new lower bound
        boundary_math :=  'SELECT '
            || old_upper_bound || ' - ' || lower_bound_overlap
            || ' AS new_lower_bound, pg_typeof(' || old_upper_bound || ') AS new_type';
        EXECUTE boundary_math INTO STRICT new_lower_bound, new_type;

        factor := constrained_column || ' < ''' || new_lower_bound || '''::' || new_type;

        -- generate a where clause factor and append it to the current clause.
        IF where_clause IS NULL
        THEN
            where_clause := factor;
        ELSE
            where_clause := where_clause || ' AND ' || factor;
        END IF;
    END LOOP;

   -- Did we find any columns with overlap bounds? Otherwise, there's no data to move.
    IF where_clause IS NULL THEN
        RETURN 0;
    END IF;

    -- move data out of child partition to limbo table
    child_name := rolling_window.child_name(parent, lower_bound);

    insert_str := 'INSERT INTO ' || quote_ident(parent_namespace) || '.' || quote_ident(parent || '_limbo')
        || ' SELECT * FROM ' || quote_ident(parent_namespace) || '.' || quote_ident(child_name)
        || ' WHERE ' || where_clause;
    EXECUTE insert_str;
    GET DIAGNOSTICS insert_count = ROW_COUNT;

    delete_str := 'DELETE FROM ' || quote_ident(parent_namespace) || '.' || quote_ident(child_name)
        || ' WHERE ' || where_clause;
    EXECUTE delete_str;
    GET DIAGNOSTICS delete_count = ROW_COUNT;

    IF insert_count != delete_count THEN
        RAISE EXCEPTION 'Inserted % rows, but attempted to delete % rows.', insert_count, delete_count;
    END IF;

    RETURN insert_count;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION move_data_below_lower_bound_overlap_to_limbo(name, name, bigint)
IS 'If we have defined allowable overlaps for this column, and there is a prior-partition with a boundary, remove to the limbo table any data that lies outside this boundary.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION constrain_partition(
    parent_namespace name,
    parent name,
    lower_bound bigint,
    column_to_be_constrained name
) RETURNS name AS $definition$
DECLARE
    child name;
    name_of_constraint name;
    find_bounds_sql text;
    constraint_upper_bound_as_text text;
    constraint_lower_bound_as_text text;
    constraint_sql text;
BEGIN
    child := rolling_window.child_name(parent, lower_bound);
    name_of_constraint := 'bound_' || column_to_be_constrained;
    -- TODO: this relies on the text representation of the columns being cast to the appropriate type. sketchy.
    find_bounds_sql := 'SELECT CAST(min(' || quote_ident(column_to_be_constrained) || ') AS text) AS lower_bound, '
        || 'CAST(max(' || quote_ident(column_to_be_constrained) || ') AS text) AS upper_bound '
        || 'FROM ' || quote_ident(parent_namespace) || '.' || quote_ident(child);
    EXECUTE find_bounds_sql INTO constraint_lower_bound_as_text, constraint_upper_bound_as_text;
    IF constraint_upper_bound_as_text IS NULL
    THEN
        RAISE EXCEPTION 'max(%) is NULL. Is the table empty, or does it have any non-null values in the column to be constrained?', column_to_be_constrained;
    END IF;
    constraint_sql := 'ALTER TABLE ' ||  quote_ident(parent_namespace) || '.' || quote_ident(child)
        || ' ADD CONSTRAINT ' || quote_ident(name_of_constraint)
        || ' CHECK (' || quote_ident(column_to_be_constrained)
        || ' BETWEEN ' || quote_literal(constraint_lower_bound_as_text)
        || ' AND ' || quote_literal(constraint_upper_bound_as_text)
        || ')';
    EXECUTE constraint_sql;
    RETURN name_of_constraint;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION constrain_partition(name, name, bigint, name)
IS 'Find the upper and lower bound for the constrained_column in the partition and add that as a table constraint.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION freeze_partition(
    parent_namespace name,
    parent name,
    lower_bound bigint
) RETURNS SETOF name AS $definition$
DECLARE
    parent_oid oid;
    child_oid oid;
    namespace_oid oid;
    freeze_column name;
    missing_constraint_query_str text;
BEGIN
    PERFORM rolling_window.move_data_below_lower_bound_overlap_to_limbo(parent_namespace, parent, lower_bound);
    SELECT c.oid, n.oid
        INTO parent_oid, namespace_oid
        FROM pg_catalog.pg_class c
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
        AND n.nspname = parent_namespace;
    SELECT c.oid
        INTO child_oid
        FROM pg_catalog.pg_class c
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = rolling_window.child_name(parent, lower_bound)
          AND n.nspname = parent_namespace;
    missing_constraint_query_str := $q$
        SELECT ctf.column_name
        FROM rolling_window.columns_to_freeze ctf
        WHERE ctf.relid = $1    -- parent oid
        EXCEPT
        SELECT substr(c.conname, 7)
        FROM pg_catalog.pg_constraint c
        WHERE c.connamespace = $2   -- namespace oid
          AND c.conrelid = $3       -- child oid
          AND substr(c.conname, 1, 6) = 'bound_'
    $q$;
    FOR freeze_column IN EXECUTE missing_constraint_query_str USING parent_oid, namespace_oid, child_oid
    LOOP
        RETURN NEXT rolling_window.constrain_partition(parent_namespace, parent, lower_bound, freeze_column);
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION freeze_partition(name, name, bigint)
IS 'Add any missing boundary constraints for all columns listed in columns_to_freeze for the table. ';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION highest_freezable(
    parent_namespace name,
    parent name
) RETURNS name AS $definition$
DECLARE
    data_lag_window bigint;
    child_name name;
    reltuples real;
    non_empty_partitions bigint := 0;
BEGIN
    SELECT m.data_lag_window
        INTO data_lag_window
        FROM rolling_window.maintained_table m
        INNER JOIN pg_catalog.pg_class c ON (m.relid = c.oid)
        INNER JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid)
        WHERE c.relname = parent
          AND n.nspname = parent_namespace;
    IF data_lag_window IS NULL
    THEN
        RAISE EXCEPTION 'table not found in rolling_window.maintained_table';
    END IF;
    FOR child_name, reltuples IN
        SELECT p.relname, p.reltuples
        FROM rolling_window.list_partitions(parent_namespace, parent) AS p
        WHERE p.relname ~ (parent || E'_\\d+$')
        ORDER BY p.relname DESC
    LOOP        -- step through the partitions going from highest to lowest
        IF non_empty_partitions < data_lag_window
        THEN
            IF non_empty_partitions > 0     -- if I have seen any data in a partition before...
            THEN                            -- continue counting
                non_empty_partitions := non_empty_partitions + 1;
            ELSE
                IF reltuples > 0            -- otherwise find the first partition that contains data
                THEN
                    non_empty_partitions := 1;  -- and start counting.
                END IF;
            END IF;
        ELSE
            RETURN child_name;
        END IF;
    END LOOP;
    -- it is possible that we won't find _any_ partitions eligible for freezing.
    RETURN NULL;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION highest_freezable(name, name)
IS 'Find the highest partition that is eligible for freezing.';


---------------------------------------------------------------------
CREATE TYPE freeze_result AS (
    partition_table_name name,
    new_constraint name
);


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION freeze(
    parent_namespace name,
    parent name
) RETURNS SETOF freeze_result AS $definition$
DECLARE
    highest_freezable name;
    child_relid oid;
    child_name name;
    new_constraint name;
    f_result rolling_window.freeze_result;
    lower_bound bigint;
BEGIN
    highest_freezable := rolling_window.highest_freezable(parent_namespace, parent);
    FOR child_relid, child_name IN
        SELECT p.partition_table_oid, p.relname
        FROM rolling_window.list_partitions(parent_namespace, parent) AS p
        WHERE p.relname ~ (parent || E'_\\d+$')
          AND p.relname <= highest_freezable
        ORDER BY p.relname
    LOOP
        lower_bound := rolling_window.lower_bound_from_child_name(child_name);
        FOR new_constraint IN
            SELECT f.p
            FROM rolling_window.freeze_partition(
                parent_namespace,
                parent,
                lower_bound)
            AS f(p)
        LOOP
            f_result = ROW(child_name, new_constraint);
            RETURN NEXT f_result;
        END LOOP;
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION freeze(name, name)
IS 'Add boundary constraints for all columns ';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_freeze_column(
    my_relid oid,
    my_column_name name,
    my_lower_bound_overlap text
) RETURNS boolean AS $definition$
BEGIN
    UPDATE rolling_window.columns_to_freeze
    SET lower_bound_overlap = my_lower_bound_overlap
    WHERE relid = my_relid
      AND column_name = my_column_name;
    IF found THEN
        RETURN true;
    END IF;

    BEGIN
        INSERT INTO rolling_window.columns_to_freeze (relid, column_name, lower_bound_overlap)
        VALUES (my_relid, my_column_name, my_lower_bound_overlap);
        RETURN false;
    EXCEPTION WHEN unique_violation
    THEN
        -- do nothing, loop and try update again.
    END;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON FUNCTION set_freeze_column(oid, name, text)
IS 'Set a freeze column. Returns true if UPDATEd, false if INSERTed. This would be a single call to MERGE if PostgreSQL supported it.';
