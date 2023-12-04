---------------------- 4.1 -----------------------

CREATE OR REPLACE PROCEDURE drop_table_name_prefixed_tables()
AS $$
    DECLARE
        tables varchar[];
        tbl varchar;
    BEGIN
        SELECT array_agg(table_schema || '.' || table_name)
          FROM information_schema.tables
         WHERE table_name ILIKE 'TableName%'
          INTO tables;

        FOREACH tbl IN ARRAY tables
        LOOP
            EXECUTE FORMAT('DROP TABLE IF EXISTS %s CASCADE', tbl);
        END LOOP;
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM information_schema.tables WHERE table_name ILIKE '%TableName%';

CREATE TABLE IF NOT EXISTS TableName1 (a varchar);
CREATE TABLE IF NOT EXISTS SomePrefixTableName (a varchar);

SELECT * FROM information_schema.tables WHERE table_name ILIKE '%TableName%';

CALL drop_table_name_prefixed_tables();

SELECT * FROM information_schema.tables WHERE table_name ILIKE '%TableName%';

DROP TABLE IF EXISTS SomePrefixTableName;

---------------------- 4.2 -----------------------

CREATE OR REPLACE PROCEDURE list_user_defined_scalar_functions(OUT funcs_count integer)
AS $$
    DECLARE
        funcs varchar[];
        func varchar;
    BEGIN
       SELECT ARRAY (
           SELECT FORMAT('%s(%s) RETURNS %s', p.proname, pg_get_function_arguments(p.oid), format_type(t.oid, t.typtypmod))
             FROM pg_proc p
                  LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
                  LEFT JOIN pg_type t ON p.prorettype = t.oid
                  LEFT JOIN pg_language l ON p.prolang = l.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND NOT p.proretset AND p.prorettype != 0
                  AND pg_get_function_arguments(p.oid) != ''
                  AND l.lanname = 'sql'
            ORDER BY p.proname, pg_get_function_arguments(p.oid)
       )
       INTO funcs;

       funcs_count = 0;

       FOREACH func IN ARRAY funcs
       LOOP
            RAISE INFO '%', func;
            funcs_count = funcs_count + 1;
       END LOOP;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE output_list_scalar_functions_result()
AS $$
    DECLARE
        n integer;
    BEGIN
       CALL list_user_defined_scalar_functions(n);
       RAISE INFO '%', n;
    END;
$$ LANGUAGE plpgsql;

CALL output_list_scalar_functions_result();

CREATE OR REPLACE FUNCTION abobochka() RETURNS integer
AS $$
    SELECT 1;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION abobochka2(integer) RETURNS integer
AS $$
    SELECT $1;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION abobochka3(integer) RETURNS integer[]
AS $$
    SELECT ARRAY(SELECT $1);
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION abobochka_skip(integer) RETURNS TABLE(a integer)
AS $$
    SELECT $1;
$$ LANGUAGE sql;

CREATE TYPE my_enum AS ENUM ('one', 'two');

CREATE OR REPLACE FUNCTION abobochka_enum(my_enum) RETURNS my_enum[]
AS $$
    SELECT ARRAY(SELECT $1);
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION abobochka_plpgsql(integer) RETURNS integer
AS $$
    BEGIN
       RETURN $1;
    END;
$$ LANGUAGE plpgsql;

CALL output_list_scalar_functions_result();

DROP FUNCTION abobochka();
DROP FUNCTION abobochka2(integer);
DROP FUNCTION abobochka3(integer);
DROP FUNCTION abobochka_skip(integer);

DROP FUNCTION abobochka_enum(my_enum);
DROP TYPE my_enum;

DROP FUNCTION abobochka_plpgsql(integer);

---------------------- 4.3 -----------------------

CREATE OR REPLACE PROCEDURE drop_all_dml_triggers(OUT trg_count integer)
AS $$
    DECLARE
        current_trg RECORD;
    BEGIN
        trg_count = 0;
        FOR current_trg IN SELECT DISTINCT trigger_name, event_object_table, event_object_schema
                             FROM information_schema.triggers
        LOOP
            EXECUTE 'DROP TRIGGER ' || current_trg.trigger_name ||
                ' ON ' || current_trg.event_object_schema || '.' || current_trg.event_object_table || ';';
            trg_count = trg_count + 1;
        END LOOP;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE output_drop_all_dml_triggers_result()
AS $$
    DECLARE
        n integer;
    BEGIN
       CALL drop_all_dml_triggers(n);
       RAISE INFO '%', n;
    END;
$$ LANGUAGE plpgsql;

CALL output_drop_all_dml_triggers_result();

SELECT * FROM information_schema.triggers;

CREATE TABLE triggered (a integer);

CREATE OR REPLACE FUNCTION aboba_trigger() RETURNS trigger
AS $$
    BEGIN
       IF TG_OP IN ('INSERT', 'UPDATE') THEN
           RETURN NEW;
       ELSE
           RETURN OLD;
       END IF;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER aboba_before_trigger
BEFORE INSERT OR UPDATE OR DELETE
ON triggered
FOR EACH ROW EXECUTE FUNCTION aboba_trigger();

CREATE TRIGGER aboba_after_trigger
AFTER INSERT OR UPDATE OR DELETE
ON triggered
FOR EACH ROW EXECUTE FUNCTION aboba_trigger();

SELECT * FROM information_schema.triggers;

CALL output_drop_all_dml_triggers_result();

SELECT * FROM information_schema.triggers;

DROP TABLE triggered;
DROP FUNCTION aboba_trigger();

---------------------- 4.4 -----------------------

CREATE OR REPLACE PROCEDURE output_funcs_and_procs_with_string(varchar)
AS $$
    DECLARE
        func RECORD;
    BEGIN
       FOR func IN SELECT proname AS name, pg_get_functiondef(oid) AS definition
                     FROM pg_proc
                    WHERE proname LIKE '%' || $1 || '%' AND NOT proretset
       LOOP
           RAISE INFO E'\n\n\nName: %\nDefinition:\n%', func.name, func.definition;
       END LOOP;
    END;
$$ LANGUAGE plpgsql;

CALL output_funcs_and_procs_with_string('dml');

CALL output_funcs_and_procs_with_string('dql');

CALL output_funcs_and_procs_with_string('list_scalar');

CREATE OR REPLACE FUNCTION test_function_megaabobus_2000(integer, varchar) RETURNS bool[]
AS $$
    BEGIN
       RETURN '{true, false}'::bool[];
    END;
$$ LANGUAGE plpgsql;

CALL output_funcs_and_procs_with_string('megaabobus');

DROP FUNCTION test_function_megaabobus_2000(integer, varchar);