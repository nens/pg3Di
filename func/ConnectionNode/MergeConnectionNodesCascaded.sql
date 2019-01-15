-------------------------------------------------------------------
-- MERGE CONNECTION NODES, RECONNECT LINES BASED ON ID AND OTHER ID
-- INPUT:	- v2_model
-- 		- delete_id and merge_id (id will be removed)
-- OUTPUT:	- updated v2_model:
-- 		- removed connection_nodes and manholes of id
-- 		- update connection_node_id of structures, impervious_map and laterals to other_id
------------------------------------------------------------------- 

---- ZORG DAT JE EERST REMOVE TABLES HEBT AANGEMAAKT: -----
-- SELECT create_schema_with_3di_remove_tabels();

CREATE OR REPLACE FUNCTION merge_connection_nodes_cascaded(delete_id integer, merge_id integer)
RETURNS void AS
$$
DECLARE
    row record;
    x record;
BEGIN
    -- remove manhole
    EXECUTE 'WITH moved_rows AS(
		DELETE FROM public.v2_manhole WHERE connection_node_id = ' || delete_id || '
		RETURNING *
	     ) 
	     INSERT INTO rm.v2_manhole SELECT * FROM moved_rows;';
    -- update all connection_node_ids to merge_id
    FOR row IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND (tablename LIKE 'v2_%')   -- and other conditions, if needed
    LOOP
	FOR x IN SELECT column_name FROM information_schema.columns where table_name LIKE row.tablename AND (column_name LIKE 'connection_node%id')
	LOOP
		RAISE NOTICE '% delete_id % merge_id in: % %', delete_id, merge_id, row, x;
		EXECUTE 'UPDATE public.' || quote_ident(row.tablename) || ' SET ' || quote_ident(x.column_name) || ' = ' || merge_id || ' WHERE ' || quote_ident(x.column_name) || ' = ' || delete_id || ';';
	END LOOP;
    END LOOP;
    -- remove connection_node
    EXECUTE 'WITH moved_rows AS(
		DELETE FROM public.v2_connection_nodes WHERE id = ' || delete_id || '
		RETURNING *
	     ) 
	     INSERT INTO rm.v2_connection_nodes SELECT * FROM moved_rows;';
END;
$$ LANGUAGE plpgsql;

---- VOORBEELD HOE DEZE FUNCTIE TE GEBRUIKEN: -----
-- SELECT merge_connection_nodes_cascaded(delete_id, merge_id)
-- FROM table
-- WHERE SOME STATEMENTS;