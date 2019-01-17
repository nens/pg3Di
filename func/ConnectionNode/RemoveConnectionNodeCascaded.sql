----------------------------------------------------------------
--- REMOVE CONNECTION NODES CASCADED
--- Input: 3Di database with rm_tables + v2_connection_nodes.ids
--- Output: Removed items in 'rm' tabels
----------------------------------------------------------------

---- ZORG DAT JE EERST REMOVE TABLES HEBT AANGEMAAKT: -----
-- SELECT create_schema_with_3di_remove_tabels();

CREATE OR REPLACE FUNCTION remove_connection_nodes_cascaded(id integer)
RETURNS void AS
$$
    DECLARE
        row record;
        x record;
    BEGIN
        FOR row IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND (tablename LIKE 'v2_%')   -- and other conditions, if needed
        LOOP
        FOR x IN SELECT column_name FROM information_schema.columns where table_name LIKE row.tablename AND (column_name LIKE 'connection_node%id')
        LOOP
            --RAISE NOTICE '% %', row, x;
            EXECUTE 'WITH moved_rows AS(
                    DELETE FROM public.' || quote_ident(row.tablename) || ' WHERE ' || quote_ident(x.column_name) || ' = ' || id || '
                    RETURNING *
                ) 
                INSERT INTO rm.' || quote_ident(row.tablename) || ' SELECT * FROM moved_rows;';
        END LOOP;
        END LOOP;
        EXECUTE 'WITH moved_rows AS(
            DELETE FROM public.v2_connection_nodes WHERE id = ' || id || '
            RETURNING *
             ) 
             INSERT INTO rm.v2_connection_nodes SELECT * FROM moved_rows;';
    END;
$$ LANGUAGE plpgsql;

---- VOORBEELD HOE DEZE FUNCTIE TE GEBRUIKEN: -----
-- SELECT remove_connection_nodes_cascaded(id)
-- FROM v2_connection_nodes
-- WHERE SOME STATEMENTS;

-- SELECT remove_connection_nodes_cascaded(4087);