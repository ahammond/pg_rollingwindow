-- do not allow inserts into parent tables.

CREATE SCHEMA rollingwindow;
SET search_path TO rollingwindow,public;

CREATE TABLE maintained_table (
  parent text PRIMARY KEY,          -- use & reference pg_class.oid
  partition_column text NOT NULL,   -- use & reference pg_attribute.oid
  step bigint NOT NULL,
  partitions_to_keep bigint NOT NULL,
  partitioned_on TIMESTAMP NOT NULL DEFAULT NOW(),
  rolled_on TIMESTAMP NOT NULL DEFAULT NOW(),
  lower_bound_of_lowest_partition bigint NOT NULL DEFAULT(0)
);
COMMENT ON TABLE maintained_table
IS 'Store metadata for pg_rollingwindow';
COMMENT ON COLUMN maintained_table.parent
IS 'name of the table being managed';
COMMENT ON COLUMN maintained_table.partition_column
IS 'name of the partition key column';
COMMENT ON COLUMN maintained_table.step
IS 'size of increment between partition tables';
COMMENT ON COLUMN maintained_table.partitions_to_keep
IS 'drop oldest partitions when we have more than this many partition child tables';
COMMENT ON COLUMN maintained_table.partitioned_on
IS 'when we partitioned this table';
COMMENT ON COLUMN maintained_table.rolled_on
IS 'when we last did something while maintaining this table';
COMMENT ON COLUMN maintained_table.lower_bound_of_lowest_partition
IS 'which partition should I drop the next time I need to drop one?';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION child_name(
    parent name,
    lower_bound bigint
) RETURNS name AS $definition$
SELECT CAST($1 || '_' || $2 AS name) AS child
$definition$ LANGUAGE sql;
COMMENT ON FUNCTION child_name(name, bigint)
IS 'Given the name of the parent table and the lower_boundary of the partition, return the name of the associated child partition table.';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION add_partition(
    parent name,
    partition_column_name name,
    step bigint,
    lower_bound bigint
) RETURNS name AS $definition$
DECLARE
    child name;
    upper_bound bigint;
    create_str text;
BEGIN
    child := child_name(parent, lower_bound);
    upper_bound := lower_bound + step - 1;
    create_str := 'CREATE TABLE '
        || quote_ident(child)
        || ' ( CHECK ( '|| quote_ident(partition_column_name)|| ' BETWEEN '|| lower_bound || ' AND '|| upper_bound || ' ) ) '
        || 'INHERITS ( ' || quote_ident(parent) || ' )';
    EXECUTE create_str;
    RETURN child;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON add_partition(name, name, bigint, bigint)
IS '';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION add_partitions(
    parent name,
    partition_column_name name,
    step bigint,
    start bigint,
    stop bigint
) RETURNS SETOF name AS $definition$
DECLARE
BEGIN
    FOR lower_bound IN start..stop BY step LOOP
        RETURN NEXT add_partition(parent, partition_column_name, step, lower_bound);
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON add_partitions(name, name, bigint, bigint, bigint)
IS '';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION clone_indexes_to_partition(
    parent name,
    lower_bound bigint
) RETURNS void AS $definition$
DECLARE
    child text;
    index_oid oid;
    index_query_str text := $q$SELECT indexrelid FROM pg_index WHERE indrelid = (SELECT oid FROM pg_class WHERE relname = $1 AND relkind = 'r')$q$;
    parent_index_str text;
    child_index_str text;
BEGIN
    child := child_name(parent, lower_bound);
    FOR index_oid IN EXECUTE index_query_str USING parent LOOP
        parent_index_str := pg_get_indexdef(index_oid);
        child_index_str := replace(parent_index_str, parent, child);
        EXECUTE child_index_str;
    END LOOP;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON clone_indexes_to_partition(name, bigint)
IS '';


---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION move_newest_data_to_partition(
    parent name,
    partition_column_name name,
    step bigint
) RETURNS bigint AS $definition$
DECLARE
    max_value bigint;
    lower_bound bigint;
    child text;
    select_max_value_str text;
    insert_str text;
    delete_str text;
    delete_count bigint;
BEGIN
    select_max_value_str := 'SELECT max(' || quote_ident(partition_column_name)
        || ') FROM ONLY ' || quote_ident(parent);
    EXECUTE select_max_value_str INTO STRICT max_value;
    IF max_value IS NULL THEN
        RETURN 0;
    END IF;
    lower_bound := max_value - max_value % step;
    child := child_name(parent, lower_bound);
    insert_str := 'INSERT INTO ' || quote_ident(child)
        || ' SELECT * FROM ONLY ' || quote_ident(parent)
        || ' WHERE ' || quote_ident(partition_column_name) || ' >= $1';
    EXECUTE insert_str USING lower_bound;
    delete_str := 'DELETE FROM ONLY ' || quote_ident(parent)
        || ' WHERE ' || quote_ident(partition_column_name)
        || ' IN ( SELECT ' || quote_ident(partition_column_name)
        || ' FROM ' || quote_ident(child)
        || ')';
    EXECUTE delete_str;
    GET DIAGNOSTICS delete_count = ROW_COUNT;
    PERFORM clone_indexes_to_partition(parent, lower_bound);
    RETURN delete_count;
END;
$definition$ LANGUAGE plpgsql;
COMMENT ON move_newest_data_to_partition(name, name, bigint)
IS '';



