/*

DESCRIPTION: 
It replaces a line object with another line object
The attributes of the new object will be chosen using this hierarchy:
    -- 1. Your input (values in the object_to_insert argument)
    -- 2. Object to replace (e.g. if the connection_node_start_id in object_to_insert argument is NULL, then the connection_node_start_id of the original line object is used)
    -- 3. Default values in the object_to_insert 3Di table

INPUTS:
- 	object_type_to_replace. The type of the object to be removed. One of: 'v2_channel', 'v2_culvert', 'v2_weir', 'v2_orifice', 'v2_pumpstation', 'v2_pipe'
-	object_id_to_replace. The id of the object to be removed
-	object_to_insert. A row of values of the object to be inserted, see Examples for syntax. 
        Must include all fields in the same order as defined in the database (Tip: use Scripts > Insert script). 
        Use explicit type casts to be sure that the right object can be chosen (see examples)

OUTPUTS: 
-   The function returns the id of the object that is inserted
-   If the object to replace is not found, it raises an exception
-   The function deletes the object to replace, no backup of this object is saved

DEPENDENCIES:
-   pg3Di type: pg3Di_LineObjectString
-   3Di database

REMARKS: 
- For the objects to be replaced, any Line Object can be chosen
- For the object to be inserted, so far only v2_orifice has been implemented

EXAMPLE(S):
- Replace channel 383 with an orifice with some properties given as input
        
        SELECT ReplaceLine('v2_channel', 383, (NULL, NULL, NULL, NULL, 4.3, FALSE, 1, 0.026, 2, 0.8, 0.8, 4, 4, NULL, NULL)::v2_orifice);

*/
DROP FUNCTION IF EXISTS ReplaceLine(varchar(64), integer, v2_orifice);
DROP FUNCTION IF EXISTS ReplaceLine(pg3Di_LineObjectString, integer, record);

------------------------ V2_ORIFICE -----------------------------------------

CREATE OR REPLACE FUNCTION ReplaceLine (
	object_type_to_replace pg3Di_LineObjectString,
	object_id_to_replace integer,
	object_to_insert v2_orifice
)
RETURNS
	integer
AS
$BODY$
	DECLARE 
        nr_objects_to_replace integer;
        field text;
		insert_type text;
        insert_id integer;
	BEGIN
	
		insert_type := 'v2_orifice';
		
        EXECUTE format('SELECT count(*) FROM %I WHERE id = %s;', object_type_to_replace, object_id_to_replace) INTO nr_objects_to_replace;
        
        IF nr_objects_to_replace != 1
        THEN    RAISE EXCEPTION 'No % with id % exists. Cannot replace.', object_type_to_replace, object_id_to_replace;
                RETURN NULL;
        END IF;
        
		-- Get field names, types, and defaults for object to insert
		DROP TABLE IF EXISTS object_to_insert_values;
		CREATE TEMP TABLE object_to_insert_values (
                attname text, 
                atttype text, 
                object_to_replace_value text, 
                default_value text
        ) ON COMMIT DROP;
        
        INSERT INTO object_to_insert_values (attname, atttype, default_value)
		SELECT  a.attname, 
                t.typname,
                COALESCE(pg_get_expr(d.adbin, d.adrelid), 'NULL') AS default_value
		FROM    pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_type AS t
			ON a.atttypid = t.oid
		LEFT JOIN pg_catalog.pg_attrdef d 
            ON  (a.attrelid, a.attnum) = (d.adrelid,  d.adnum)
		WHERE   NOT a.attisdropped   -- no dropped (dead) columns
		        AND    a.attnum > 0         -- no system columns
		        AND    a.attrelid = ('public.'||insert_type)::regclass
		;
        
        -- Put extra quotes around values in text-ish columns to avoid problems with quoting in EXECUTE statements
        UPDATE object_to_insert_values
        SET default_value = ''''||default_value||''''
        WHERE   atttype IN ('char', 'varchar', 'text')
                AND default_value != 'NULL'
        ;
        
        -- Get the values for fields that occur in both object_to_replace and object_to_insert
		FOR field IN (
            SELECT a.attname --, t.typname
            FROM   pg_catalog.pg_attribute AS a 
            LEFT JOIN pg_catalog.pg_type AS t
                ON a.atttypid = t.oid 	
            WHERE  NOT attisdropped   -- no dropped (dead) columns
            AND    attnum > 0         -- no system columns
            AND    attrelid = ('public.'||object_type_to_replace)::regclass
		)
		LOOP
            EXECUTE format('
                    UPDATE  object_to_insert_values 
                    SET     object_to_replace_value = (SELECT %1$I FROM %2$I WHERE id = %3$s)
                    WHERE   attname = %1$L;
                ',
                field,                  -- 1$
                object_type_to_replace, -- 2$ 
                object_id_to_replace    -- 3$
            );
  		END LOOP;

        -- Put extra quotes around values in text-ish columns to avoid problems with quoting in EXECUTE statements
		UPDATE object_to_insert_values
        SET object_to_replace_value = ''''||object_to_replace_value||''''
        WHERE   atttype IN ('char', 'varchar', 'text')
        ;

        -- Create a one-row copy of the v2_ target table to fill with the values to be inserted
        DROP TABLE IF EXISTS replaceline_object_to_insert;
        EXECUTE format('CREATE TABLE replaceline_object_to_insert AS SELECT * FROM %I LIMIT 0;', insert_type); -- Not using TEMPORARY table here, because you cannot use that with CREATE TABLE .. AS; CREATE TEMP TABLE .. (LIKE v2_orifice) would copy NOT NULL constraints, which also makes things complicated... 
        INSERT INTO replaceline_object_to_insert SELECT (object_to_insert).*;

        -- Fill that table with the values collected above, using a hiearchy:
        -- -- 1. Input (values in the object_to_insert argument)
        -- -- 2. Object to replace (e.g. if the connection_node_start_id in object_to_insert argument is NULL, then the connection_node_start_id of the original line object is used)
        -- -- 3. Default values in v2_ target table
		FOR field IN (
            SELECT a.attname
            FROM   pg_catalog.pg_attribute AS a 
            LEFT JOIN pg_catalog.pg_type AS t
                ON a.atttypid = t.oid 	
            WHERE  NOT attisdropped   -- no dropped (dead) columns
            AND    attnum > 0         -- no system columns
            AND    attrelid = ('public.'||insert_type)::regclass
		)
		LOOP            
            EXECUTE format('
                    UPDATE replaceline_object_to_insert 
                    SET %1$I = %2$s
                    WHERE %1$I IS NULL;
                ', 
                field,                  -- 1$
                (SELECT COALESCE(object_to_replace_value, default_value) FROM object_to_insert_values WHERE attname = field) -- 2$
            );
		END LOOP;

        -- Prevent that the pk is copied from the object_to_replace
        -- Instead, take either the input id value or the nextval in the pk sequence
        UPDATE  replaceline_object_to_insert
        SET     id = COALESCE((object_to_insert).id, nextval(insert_type||'_id_seq'));

        -- Remove cross section locations if object_type_to_replace = v2_channel
        -- !! We should actually fix this defining the FK of v2_channel with ON DELETE CASCADE
        IF      object_type_to_replace = 'v2_channel'
        THEN    EXECUTE format('DELETE FROM v2_cross_section_location WHERE channel_id = %s;', object_id_to_replace);
        END IF;
        
        -- Delete the object to be replaced
        EXECUTE format('DELETE FROM %I WHERE id = %s;', object_type_to_replace, object_id_to_replace);
        
        -- Insert object
        INSERT INTO v2_orifice (id, display_name, code, max_capacity, crest_level, sewerage, cross_section_definition_id, friction_value, friction_type, discharge_coefficient_positive, discharge_coefficient_negative, zoom_category, crest_type, connection_node_start_id, connection_node_end_id)
        SELECT      id, display_name, code, max_capacity, crest_level, sewerage, cross_section_definition_id, friction_value, friction_type, discharge_coefficient_positive, discharge_coefficient_negative, zoom_category, crest_type, connection_node_start_id, connection_node_end_id
        FROM        replaceline_object_to_insert
        ;                                                              
        
        -- Save the id (for RETURN) before dropping the temp table
        SELECT id FROM replaceline_object_to_insert INTO insert_id;

        -- Drop the temp table
        DROP TABLE IF EXISTS replaceline_object_to_insert;
        
        RETURN insert_id;
	END;
$BODY$ LANGUAGE plpgsql;

------------------------------------ V2_CULVERT --------------------------------------
CREATE OR REPLACE FUNCTION ReplaceLine (
	object_type_to_replace pg3Di_LineObjectString,
	object_id_to_replace integer,
	object_to_insert v2_culvert
)
RETURNS
	integer
AS
$BODY$
	DECLARE 
        nr_objects_to_replace integer;
        field text;
		insert_type text;
        insert_id integer;
	BEGIN
	
		insert_type := 'v2_culvert';
		
        EXECUTE format('SELECT count(*) FROM %I WHERE id = %s;', object_type_to_replace, object_id_to_replace) INTO nr_objects_to_replace;
        
        IF nr_objects_to_replace != 1
        THEN    RAISE EXCEPTION 'No % with id % exists. Cannot replace.', object_type_to_replace, object_id_to_replace;
                RETURN NULL;
        END IF;
        
		-- Get field names, types, and defaults for object to insert
		DROP TABLE IF EXISTS object_to_insert_values;
		CREATE TEMP TABLE object_to_insert_values (
                attname text, 
                atttype text, 
                object_to_replace_value text, 
                default_value text
        ) ON COMMIT DROP;
        
        INSERT INTO object_to_insert_values (attname, atttype, default_value)
		SELECT  a.attname, 
                t.typname,
                COALESCE(pg_get_expr(d.adbin, d.adrelid), 'NULL') AS default_value
		FROM    pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_type AS t
			ON a.atttypid = t.oid
		LEFT JOIN pg_catalog.pg_attrdef d 
            ON  (a.attrelid, a.attnum) = (d.adrelid,  d.adnum)
		WHERE   NOT a.attisdropped   -- no dropped (dead) columns
		        AND    a.attnum > 0         -- no system columns
		        AND    a.attrelid = ('public.'||insert_type)::regclass
		;
        
        -- Put extra quotes around values in text-ish columns to avoid problems with quoting in EXECUTE statements
        UPDATE object_to_insert_values
        SET default_value = ''''||default_value||''''
        WHERE   atttype IN ('char', 'varchar', 'text')
                AND default_value != 'NULL'
        ;
        
        -- Get the values for fields that occur in both object_to_replace and object_to_insert
		FOR field IN (
            SELECT a.attname --, t.typname
            FROM   pg_catalog.pg_attribute AS a 
            LEFT JOIN pg_catalog.pg_type AS t
                ON a.atttypid = t.oid 	
            WHERE  NOT attisdropped   -- no dropped (dead) columns
            AND    attnum > 0         -- no system columns
            AND    attrelid = ('public.'||object_type_to_replace)::regclass
		)
		LOOP
            EXECUTE format('
                    UPDATE  object_to_insert_values 
                    SET     object_to_replace_value = (SELECT %1$I FROM %2$I WHERE id = %3$s)
                    WHERE   attname = %1$L;
                ',
                field,                  -- 1$
                object_type_to_replace, -- 2$ 
                object_id_to_replace    -- 3$
            );
  		END LOOP;

        -- Put extra quotes around values in text-ish columns to avoid problems with quoting in EXECUTE statements
		UPDATE object_to_insert_values
        SET object_to_replace_value = ''''||object_to_replace_value||''''
        WHERE   atttype IN ('char', 'varchar', 'text', 'geometry')
        ;

        -- Create a one-row copy of the v2_ target table to fill with the values to be inserted
        DROP TABLE IF EXISTS replaceline_object_to_insert;
        EXECUTE format('CREATE TABLE replaceline_object_to_insert AS SELECT * FROM %I LIMIT 0;', insert_type); -- Not using TEMPORARY table here, because you cannot use that with CREATE TABLE .. AS; CREATE TEMP TABLE .. (LIKE v2_orifice) would copy NOT NULL constraints, which also makes things complicated... 
        INSERT INTO replaceline_object_to_insert SELECT (object_to_insert).*;

        -- Fill that table with the values collected above, using a hiearchy:
        -- -- 1. Input (values in the object_to_insert argument)
        -- -- 2. Object to replace (e.g. if the connection_node_start_id in object_to_insert argument is NULL, then the connection_node_start_id of the original line object is used)
        -- -- 3. Default values in v2_ target table
		FOR field IN (
            SELECT a.attname
            FROM   pg_catalog.pg_attribute AS a 
            LEFT JOIN pg_catalog.pg_type AS t
                ON a.atttypid = t.oid 	
            WHERE  NOT attisdropped   -- no dropped (dead) columns
            AND    attnum > 0         -- no system columns
            AND    attrelid = ('public.'||insert_type)::regclass
		)
		LOOP            
            EXECUTE format('
                    UPDATE replaceline_object_to_insert 
                    SET %1$I = %2$s
                    WHERE %1$I IS NULL;
                ', 
                field,                  -- 1$
                (SELECT COALESCE(object_to_replace_value, default_value) FROM object_to_insert_values WHERE attname = field) -- 2$
            );
		END LOOP;

        -- Prevent that the pk is copied from the object_to_replace
        -- Instead, take either the input id value or the nextval in the pk sequence
        UPDATE  replaceline_object_to_insert
        SET     id = COALESCE((object_to_insert).id, nextval(insert_type||'_id_seq'));

        -- Remove cross section locations if object_type_to_replace = v2_channel
        -- !! We should actually fix this defining the FK of v2_channel with ON DELETE CASCADE
        IF      object_type_to_replace = 'v2_channel'
        THEN    EXECUTE format('DELETE FROM v2_cross_section_location WHERE channel_id = %s;', object_id_to_replace);
        END IF;
        
        -- Delete the object to be replaced
        EXECUTE format('DELETE FROM %I WHERE id = %s;', object_type_to_replace, object_id_to_replace);
        
        -- Insert object
        INSERT INTO v2_culvert (id, display_name, code, calculation_type, friction_value, friction_type, dist_calc_points, zoom_category, cross_section_definition_id, discharge_coefficient_positive, discharge_coefficient_negative, invert_level_start_point, invert_level_end_point, the_geom, connection_node_start_id, connection_node_end_id)
        SELECT      id, display_name, code, calculation_type, friction_value, friction_type, dist_calc_points, zoom_category, cross_section_definition_id, discharge_coefficient_positive, discharge_coefficient_negative, invert_level_start_point, invert_level_end_point, the_geom, connection_node_start_id, connection_node_end_id
        FROM        replaceline_object_to_insert
        ;                                                              
        
        -- Save the id (for RETURN) before dropping the temp table
        SELECT id FROM replaceline_object_to_insert INTO insert_id;

        -- Drop the temp table
        DROP TABLE IF EXISTS replaceline_object_to_insert;
        
        RETURN insert_id;
	END;
$BODY$ LANGUAGE plpgsql;

------------------------------------------------ v2_pipe ---------

CREATE OR REPLACE FUNCTION ReplaceLine (
	object_type_to_replace pg3Di_LineObjectString,
	object_id_to_replace integer,
	object_to_insert v2_pipe
)
RETURNS
	integer
AS
$BODY$
	DECLARE 
        nr_objects_to_replace integer;
        field text;
		insert_type text;
        insert_id integer;
	BEGIN
	
		insert_type := 'v2_pipe';
		
        EXECUTE format('SELECT count(*) FROM %I WHERE id = %s;', object_type_to_replace, object_id_to_replace) INTO nr_objects_to_replace;
        
        IF nr_objects_to_replace != 1
        THEN    RAISE EXCEPTION 'No % with id % exists. Cannot replace.', object_type_to_replace, object_id_to_replace;
                RETURN NULL;
        END IF;
        
		-- Get field names, types, and defaults for object to insert
		DROP TABLE IF EXISTS object_to_insert_values;
		CREATE TEMP TABLE object_to_insert_values (
                attname text, 
                atttype text, 
                object_to_replace_value text, 
                default_value text
        ) ON COMMIT DROP;
        
        INSERT INTO object_to_insert_values (attname, atttype, default_value)
		SELECT  a.attname, 
                t.typname,
                COALESCE(pg_get_expr(d.adbin, d.adrelid), 'NULL') AS default_value
		FROM    pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_type AS t
			ON a.atttypid = t.oid
		LEFT JOIN pg_catalog.pg_attrdef d 
            ON  (a.attrelid, a.attnum) = (d.adrelid,  d.adnum)
		WHERE   NOT a.attisdropped   -- no dropped (dead) columns
		        AND    a.attnum > 0         -- no system columns
		        AND    a.attrelid = ('public.'||insert_type)::regclass
		;
        
        -- Put extra quotes around values in text-ish columns to avoid problems with quoting in EXECUTE statements
        UPDATE object_to_insert_values
        SET default_value = ''''||default_value||''''
        WHERE   atttype IN ('char', 'varchar', 'text')
                AND default_value != 'NULL'
        ;
        
        -- Get the values for fields that occur in both object_to_replace and object_to_insert
		FOR field IN (
            SELECT a.attname --, t.typname
            FROM   pg_catalog.pg_attribute AS a 
            LEFT JOIN pg_catalog.pg_type AS t
                ON a.atttypid = t.oid 	
            WHERE  NOT attisdropped   -- no dropped (dead) columns
            AND    attnum > 0         -- no system columns
            AND    attrelid = ('public.'||object_type_to_replace)::regclass
		)
		LOOP
            EXECUTE format('
                    UPDATE  object_to_insert_values 
                    SET     object_to_replace_value = (SELECT %1$I FROM %2$I WHERE id = %3$s)
                    WHERE   attname = %1$L;
                ',
                field,                  -- 1$
                object_type_to_replace, -- 2$ 
                object_id_to_replace    -- 3$
            );
  		END LOOP;

        -- Put extra quotes around values in text-ish columns to avoid problems with quoting in EXECUTE statements
		UPDATE object_to_insert_values
        SET object_to_replace_value = ''''||object_to_replace_value||''''
        WHERE   atttype IN ('char', 'varchar', 'text')
        ;

        -- Create a one-row copy of the v2_ target table to fill with the values to be inserted
        DROP TABLE IF EXISTS replaceline_object_to_insert;
        EXECUTE format('CREATE TABLE replaceline_object_to_insert AS SELECT * FROM %I LIMIT 0;', insert_type); -- Not using TEMPORARY table here, because you cannot use that with CREATE TABLE .. AS; CREATE TEMP TABLE .. (LIKE v2_orifice) would copy NOT NULL constraints, which also makes things complicated... 
        INSERT INTO replaceline_object_to_insert SELECT (object_to_insert).*;

        -- Fill that table with the values collected above, using a hiearchy:
        -- -- 1. Input (values in the object_to_insert argument)
        -- -- 2. Object to replace (e.g. if the connection_node_start_id in object_to_insert argument is NULL, then the connection_node_start_id of the original line object is used)
        -- -- 3. Default values in v2_ target table
		FOR field IN (
            SELECT a.attname
            FROM   pg_catalog.pg_attribute AS a 
            LEFT JOIN pg_catalog.pg_type AS t
                ON a.atttypid = t.oid 	
            WHERE  NOT attisdropped   -- no dropped (dead) columns
            AND    attnum > 0         -- no system columns
            AND    attrelid = ('public.'||insert_type)::regclass
		)
		LOOP            
            EXECUTE format('
                    UPDATE replaceline_object_to_insert 
                    SET %1$I = %2$s
                    WHERE %1$I IS NULL;
                ', 
                field,                  -- 1$
                (SELECT COALESCE(object_to_replace_value, default_value) FROM object_to_insert_values WHERE attname = field) -- 2$
            );
		END LOOP;

        -- Prevent that the pk is copied from the object_to_replace
        -- Instead, take either the input id value or the nextval in the pk sequence
        UPDATE  replaceline_object_to_insert
        SET     id = COALESCE((object_to_insert).id, nextval(insert_type||'_id_seq'));

        -- Remove cross section locations if object_type_to_replace = v2_channel
        -- !! We should actually fix this defining the FK of v2_channel with ON DELETE CASCADE
        IF      object_type_to_replace = 'v2_channel'
        THEN    EXECUTE format('DELETE FROM v2_cross_section_location WHERE channel_id = %s;', object_id_to_replace);
        END IF;
        
        -- Delete the object to be replaced
        EXECUTE format('DELETE FROM %I WHERE id = %s;', object_type_to_replace, object_id_to_replace);
        
        -- Insert object
        INSERT INTO v2_pipe (id, display_name, code, profile_num, sewerage_type, calculation_type, invert_level_start_point, invert_level_end_point, cross_section_definition_id, friction_value, friction_type, dist_calc_points, material, pipe_quality, original_length, zoom_category, connection_node_start_id, connection_node_end_id)
        SELECT      id, display_name, code, profile_num, sewerage_type, calculation_type, invert_level_start_point, invert_level_end_point, cross_section_definition_id, friction_value, friction_type, dist_calc_points, material, pipe_quality, original_length, zoom_category, connection_node_start_id, connection_node_end_id
        FROM        replaceline_object_to_insert
        ;                                                              
        
        -- Save the id (for RETURN) before dropping the temp table
        SELECT id FROM replaceline_object_to_insert INTO insert_id;

        -- Drop the temp table
        DROP TABLE IF EXISTS replaceline_object_to_insert;
        
        RETURN insert_id;
	END;
$BODY$ LANGUAGE plpgsql;
