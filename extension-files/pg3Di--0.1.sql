CREATE EXTENSION IF NOT EXISTS hstore;--- Eval
	--- Made by Andreas Dietrich https://stackoverflow.com/questions/7433201/are-there-any-way-to-execute-a-query-inside-the-string-value-like-eval-in-post 
	create or replace function eval( sql  text ) 
	returns text as $$
	declare
	  as_txt  text;
	begin
	  if  sql is null  then  return null ;  end if ;
	  execute  sql  into  as_txt ;
	  return  as_txt ;
	end;
	$$ language plpgsql;

	
-- DeleteNullKeys
	CREATE OR REPLACE FUNCTION DeleteNullKeys (input_hstore hstore)
	RETURNS
		hstore
	AS
	$BODY$
		DECLARE 
			i text;
			output_hstore hstore;
		BEGIN
			output_hstore := input_hstore;
			FOR i IN (SELECT skeys(input_hstore))
			LOOP 
				IF NOT 	defined(input_hstore, i)
				THEN 	SELECT delete(output_hstore, i);
				END IF;
            END LOOP;
			RETURN output_hstore;
		END;
	$BODY$ LANGUAGE plpgsql;


-- pg3Di_LineObjectString
    DROP TYPE IF EXISTS pg3Di_LineObjectString CASCADE;
	CREATE TYPE pg3Di_LineObjectString AS 
		enum('v2_channel', 'v2_culvert', 'v2_weir', 'v2_orifice', 'v2_pumpstation', 'v2_pipe')
	;/*

DESCRIPTION: 
<What does de function do>

INPUTS:
- <required input arguments + their description>
- [OPTIONAL] <Optional input arguments + their description>

OUTPUTS: 
- <What is returned by the function>
- <How is the database affected / what edits in which tables result from calling the function>


DEPENDENCIES:
- What functions does this function depend on that are not available in postgresql, postgis or pg3Di ?

REMARKS: 
- Anything the user should be aware of 

EXAMPLE(S):
	
	SELECT (Connections('v2_connection_nodes', 23)).*; -- find all connections to connection node 23
*/

-- DROP FUNCTION connections(character varying,integer,character varying);
CREATE OR REPLACE FUNCTION ConnectedLines (
	object_type varchar(64),
	object_id integer,
	side varchar(5) DEFAULT 'both',
	input_side OUT varchar(5), 
	connected_object_type OUT varchar(64), 
	connected_object_id OUT integer, 
	connected_object_side OUT varchar(5)
)
RETURNS
	setof record
AS
$BODY$
	DECLARE 
		cono_ids_node_obj integer[];
	BEGIN
		-- differ behaviour for node objects, line objects and invalid objects
		IF object_type IN ('v2_connection_nodes', 'v2_manhole', 'v2_surface', 'v2_impervious_surface', 'v2_1d_lateral', 'v2_1d_boundary_conditions')
			THEN 
				RAISE NOTICE 'Object type is a Node Object; ''side'' argument will be disregarded.';
				IF 	object_type = 'v2_connection_nodes'
					THEN 	SELECT array_agg(id) FROM v2_connection_nodes WHERE id = object_id INTO cono_ids_node_obj;
					ELSIF 	object_type = 'v2_manhole'
					THEN 	SELECT array_agg(id) FROM v2_connection_nodes WHERE id = (SELECT connection_node_id FROM v2_manhole WHERE id = object_id) INTO cono_ids_node_obj;
					ELSIF	object_type = 'v2_surface'
					THEN 	SELECT array_agg(id) FROM v2_connection_nodes WHERE id = (SELECT connection_node_id FROM v2_surface_map AS suma JOIN v2_surface AS su ON su.id = suma.surface_id WHERE su.id = object_id) INTO cono_ids_node_obj;
					ELSIF	object_type = 'v2_impervious_surface'
					THEN 	SELECT array_agg(id) FROM v2_connection_nodes WHERE id = (SELECT connection_node_id FROM v2_impervious_surface_map AS suma JOIN v2_impervious_surface AS su ON su.id = suma.impervious_surface_id WHERE su.id = object_id) INTO cono_ids_node_obj;
					ELSIF 	object_type = 'v2_1d_lateral'
					THEN 	SELECT array_agg(id) FROM v2_connection_nodes WHERE id = (SELECT connection_node_id FROM v2_1d_lateral WHERE id = object_id) INTO cono_ids_node_obj;
					ELSIF 	object_type = 'v2_1d_boundary_conditions'
					THEN 	SELECT array_agg(id) FROM v2_connection_nodes WHERE id = (SELECT connection_node_id FROM v2_1d_boundary_conditions WHERE id = object_id) INTO cono_ids_node_obj;
				END IF;
				
				RETURN QUERY 
				SELECT 	NULL::varchar(5) AS input_side, 
						(_ConnectedLinesForNode(unnest(cono_ids_node_obj))).*
				;

					
			ELSIF object_type IN ('v2_channel', 'v2_culvert', 'v2_weir', 'v2_orifice', 'v2_pumpstation', 'v2_pipe')
			THEN RAISE EXCEPTION 'ConnectedLines for object_type % not yet implemented', object_type;
			ELSE RAISE EXCEPTION '% is not a valid object_type', object_type;
		END IF;
		
		IF side NOT IN ('start'::varchar(5), 'end'::varchar(5), 'both'::varchar(5)) 
			THEN RAISE EXCEPTION '% is not a valid string for side argument', side USING HINT = 'Choose from ''start'', ''end'' or ''both''.';
		END IF;
		
	END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION _ConnectedLinesForNode (
	connection_node_id integer,
	connected_object_type OUT varchar(64), 
	connected_object_id OUT integer, 
	connected_object_side OUT varchar(5)
)
RETURNS
	setof record
AS
$BODY$
	BEGIN
		-- checks on inputs
		RETURN QUERY
			-- pumpstation
			SELECT 	'v2_pumpstation'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'start'::varchar(5)::varchar(5) AS connected_object_side 
			FROM 	v2_pumpstation 
			WHERE 	connection_node_start_id = connection_node_id
			UNION
			SELECT 	'v2_pumpstation'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_pumpstation 
			WHERE 	connection_node_end_id = connection_node_id
			UNION
			
			-- pipe
			SELECT 	'v2_pipe'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_pipe 
			WHERE 	connection_node_start_id = connection_node_id
			UNION
			SELECT 	'v2_pipe'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_pipe 
			WHERE 	connection_node_end_id = connection_node_id
			UNION
			
			-- weir
			SELECT 	'v2_weir'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_weir 
			WHERE 	connection_node_start_id = connection_node_id
			UNION
			SELECT 	'v2_weir'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_weir 
			WHERE 	connection_node_end_id = connection_node_id
			UNION
			
			-- orifice
			SELECT 	'v2_orifice'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_orifice 
			WHERE 	connection_node_start_id = connection_node_id
			UNION
			SELECT 	'v2_orifice'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_orifice 
			WHERE 	connection_node_end_id = connection_node_id
			UNION
			
			-- culvert
			SELECT 	'v2_culvert'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_culvert 
			WHERE 	connection_node_start_id = connection_node_id
			UNION
			SELECT 	'v2_culvert'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_culvert 
			WHERE 	connection_node_end_id = connection_node_id
			UNION
			
			-- channel
			SELECT 	'v2_channel'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_channel 
			WHERE 	connection_node_start_id = connection_node_id
			UNION
			SELECT 	'v2_channel'::varchar(64) AS connected_object_type, 
					id AS connected_object_id, 
					'end'::varchar(5)  AS connected_object_side 
			FROM 	v2_channel 
			WHERE 	connection_node_end_id = connection_node_id
		;
	END;
$BODY$ LANGUAGE plpgsql;

/* Testen
  
  SELECT (ConnectedLines('v2_manhole', 4374)).*
                                                                                
  SELECT id FROM v2_manhole ORDER BY id LIMIT 100;
                                                                                
*/
/*

DESCRIPTION: 
Returns statistics on the pipes connected to a manhole

INPUTS:
- manhole_id integer
- [OPTIONAL] <Optional input arguments + their description>

OUTPUTS: 
- Returns a compound data type (multiple columns):
	count_total
	count_mixed
	count_rain_water
	count_dry_weather
	count_transport
	count_spillway
	count_syphon
	count_storage
	count_storage_tank
	invert_level_min
	invert_level_avg
	invert_level_max  OUT double precision

- <How is the database affected / what edits in which tables result from calling the function>


DEPENDENCIES:
- What functions does this function depend on that are not available in postgresql, postgis or pg3Di ?

REMARKS: 
- Anything the user should be aware of 

EXAMPLE(S):
- -- Get stats for all manholes in model:
  SELECT id AS manhole_id, (ConnectedPipeStats (id)).* FROM v2_manhole;
  
*/

DROP FUNCTION IF EXISTS ConnectedPipeStats(integer);
CREATE OR REPLACE FUNCTION ConnectedPipeStats (
	manhole_id integer,
	count_total OUT integer,
	count_mixed OUT integer,
	count_rain_water OUT integer,
	count_dry_weather OUT integer,
	count_transport OUT integer,
	count_spillway OUT integer,
	count_syphon OUT integer,
	count_storage OUT integer,
	count_storage_tank OUT integer,
	invert_level_min  OUT double precision,
	invert_level_avg  OUT double precision,
	invert_level_max  OUT double precision
)	
RETURNS
	record
AS
$BODY$
    WITH conn_lines AS (
        SELECT (ConnectedLines('v2_manhole', manhole_id)).*
    ), 
    connected_pipes AS (
        SELECT	p.*,
                cl.connected_object_side
        FROM 	v2_pipe AS p
        JOIN 	conn_lines AS cl
            ON	p.id = cl.connected_object_id
        WHERE 	cl.connected_object_type = 'v2_pipe'

    ),
    connected_invert_levels AS (
        SELECT invert_level_start_point AS invert_level FROM connected_pipes WHERE connected_object_side = 'start' 
        UNION
        SELECT invert_level_end_point FROM connected_pipes WHERE connected_object_side = 'end' 
    ), 
    counts AS (
        SELECT 	count(*)::integer AS count_total,
                SUM( CASE WHEN sewerage_type = 0 THEN 1 ELSE 0 END )::integer AS count_mixed,
                SUM( CASE WHEN sewerage_type = 1 THEN 1 ELSE 0 END )::integer AS count_rain_water,
                SUM( CASE WHEN sewerage_type = 2 THEN 1 ELSE 0 END )::integer AS count_dry_weather,
                SUM( CASE WHEN sewerage_type = 3 THEN 1 ELSE 0 END )::integer AS count_transport,
                SUM( CASE WHEN sewerage_type = 4 THEN 1 ELSE 0 END )::integer AS count_spillway,
                SUM( CASE WHEN sewerage_type = 5 THEN 1 ELSE 0 END )::integer AS count_syphon,
                SUM( CASE WHEN sewerage_type = 6 THEN 1 ELSE 0 END )::integer AS count_storage,
                SUM( CASE WHEN sewerage_type = 7 THEN 1 ELSE 0 END )::integer AS count_storage_tank
        FROM 	connected_pipes
    ),
    invert_level_stats AS (
        SELECT 	min(invert_level)::double precision AS invert_level_min,
                avg(invert_level)::double precision AS invert_level_avg,
                max(invert_level)::double precision AS invert_level_max
        FROM 	connected_invert_levels
    )
    SELECT c.*, ils.* FROM counts AS c, invert_level_stats AS ils
	;
$BODY$ LANGUAGE sql;
                        
                        
/* TESTEN
   
    -- Get stats for all manholes in model
  SELECT id AS manhole_id, (ConnectedPipeStats (id)).* FROM v2_manhole;
  
*//*

DESCRIPTION: Returns the next or previous cross section location along the channel of the given channel id.

INPUTS:
- channel id: id of the channel of interest
- fraction: location from where to start searching, defined as a fraction (as in ST_LineLocatePoint, ST_LineInterpolatePoint)  
- [OPTIONAL] leadby: 1 for first next cross section location, 2 for the second next, -1 for first previous, etc. Defaults to 1.

OUTPUTS: 
- id of the encountered cross section location. Returns NULL if no next (or second-next, previous, etc.) cross section locations is found

DEPENDENCIES: None

REMARKS: 
- Only works in a 3Di PostGIS database. 

EXAMPLE:

	SELECT NextCrossSectionLocation(23, 0.8, -1); -- Returns the first cross section location on channel 23, starting from 80% of the length of the channel, searching in backward direction

 */

CREATE OR REPLACE FUNCTION NextCrossSectionLocation(channel_id integer, fraction double precision, leadby integer DEFAULT 1)
RETURNS
	integer
AS
$BODY$
	DECLARE 
 		result_id integer;
	BEGIN
		WITH xsec_with_fractions AS (
			SELECT 	xsec.id AS xsec_id,
					ST_LineLocatePoint(chn.the_geom, xsec.the_geom) AS frac
			FROM 	v2_cross_section_location AS xsec,
					v2_channel AS chn
			WHERE	xsec.channel_id = $1
					AND chn.id = $1
			UNION
			SELECT -1 AS xsec_id, fraction AS frac
		),
		answer AS (
			SELECT 	xsec_id, 
					lead(xsec_id, leadby) over(ORDER BY frac) AS next_xsec_id
			FROM xsec_with_fractions
		)
		SELECT next_xsec_id FROM answer WHERE xsec_id = -1
		INTO result_id
		;

		RETURN result_id;
	END;
$BODY$ LANGUAGE plpgsql;
/*

DESCRIPTION: 
<What does de function do>

INPUTS:
- <required input arguments + their description>
- [OPTIONAL] <Optional input arguments + their description>

OUTPUTS: 
- <What is returned by the function>
- <How is the database affected / what edits in which tables result from calling the function>


DEPENDENCIES:
- What functions does this function depend on that are not available in postgresql, postgis or pg3Di ?

REMARKS: 
- Anything the user should be aware of 

EXAMPLE(S):
	[1]
	-- Get the number of connections for connection node 23:
	SELECT NumConnectedLines('v2_connection_nodes', 23); 
	
	[2]
	-- Delete all connection nodes without any connections
	DELETE FROM v2_connection_nodes WHERE NumConnectedLines('v2_connection_nodes', id) = 0;

	[3]
	-- List all boundary conditions with <> 1 connection
	SELECT * FROM v2_boundary_conditions WHERE NumConnectedLines('v2_connection_nodes', connection_node_id) != 1;
*/


CREATE OR REPLACE FUNCTION NumConnectedLines (
	object_type varchar(64),
	object_id integer,
	side varchar(5) DEFAULT 'both'
)
RETURNS
	integer
AS
$BODY$
	DECLARE
		result integer;
	BEGIN
		WITH conns AS (
			SELECT (ConnectedLines(object_type, object_id, side)).*
		)
		SELECT count(*) FROM conns INTO result;
		RETURN result;
	END;
$BODY$ LANGUAGE plpgsql;
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
- Replace a pipe with a weir if the v2_weir has defaults for all columns with non-null constraints (inheriting properties from the pipe where possible):
			        
			ReplaceLine ('v2_pipe', 2134, NULL::v2_weir)

- If "sewerage" has a non-null constraint but no default:

			ReplaceLine ('v2_pipe', 2134, populate_record((NULL::v2_weir), 'sewerage=>TRUE'))    
			
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


------------------------------------------------ v2_weir ---------

CREATE OR REPLACE FUNCTION ReplaceLine (
	object_type_to_replace pg3Di_LineObjectString,
	object_id_to_replace integer,
	object_to_insert v2_weir
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
	
		insert_type := 'v2_weir';
		
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
        INSERT INTO v2_weir (id, display_name, code, crest_level, crest_type, cross_section_definition_id, sewerage, discharge_coefficient_positive, discharge_coefficient_negative, "external", zoom_category, friction_value, friction_type, connection_node_start_id, connection_node_end_id)
        SELECT      id, display_name, code, crest_level, crest_type, cross_section_definition_id, sewerage, discharge_coefficient_positive, discharge_coefficient_negative, "external", zoom_category, friction_value, friction_type, connection_node_start_id, connection_node_end_id
        FROM        replaceline_object_to_insert
        ;                                                              
        
        -- Save the id (for RETURN) before dropping the temp table
        SELECT id FROM replaceline_object_to_insert INTO insert_id;

        -- Drop the temp table
        DROP TABLE IF EXISTS replaceline_object_to_insert;
        
        RETURN insert_id;
	END;
$BODY$ LANGUAGE plpgsql;
/*

DESCRIPTION: This function splits a channel at one or more locations. It adds the required connection nodes and a cross section location just before and after the cut location. 
- Initial waterlevel in the added connection nodes is interpolated from the existing connection nodes. 
- Reference and bank levels of the cross section locations are interpolated from the previous and next cross section location on the channel. 
- Cross section definition and friction properties are taken from the nearest cross section location. 
- All other properties are taken from the input channel.

INPUTS:
- channel_id integer id of the channel that is to be split
- locations geometry Point or MultiPoint indicating where to split the channel. Must be on the channel or within 'tolerance' of the channel
- [OPTIONAL] tolerance: determines which 'locations' are used in the analysis. Defaults to 1 mm
- [OPTIONAL] vertex_add_dist: determines at what distance from the split location cross section locations are added. If a vertex is already present within 'vertex_add_dist' from the split location, this vertex is used. Defaults to 1m

OUTPUTS: 
- Deletes the input channel from the v2_channel table
- Inserts the split channels
- Inserts the required extra connection nodes into the v2_connection_nodes table
- Inserts cross_section_locations into the v2_cross_section_location table

DEPENDENCIES: ST_LineSubstrings

REMARKS: 
- Only works in a 3Di PostGIS database. Does not keep a copy of the original channel.
- Sequences of v2_channel, v2_connection_nodes and v2_cross_section_location ids have to be up to date. If not, you may fix this like this:

    SELECT max(id) FROM v2_channel; --91
    ALTER SEQUENCE v2_channel_id_seq RESTART WITH 92;

    SELECT max(id) FROM v2_connection_nodes; --1028454
    ALTER SEQUENCE v2_connection_nodes_id_seq RESTART WITH 1028455;
    
    SELECT max(id) FROM v2_cross_section_location; --469
    ALTER SEQUENCE v2_cross_section_location_id_seq RESTART WITH 470;

- Known issues: 
		- If multiple connection nodes are encountered at the location of the split point, a v2_channel will be inserted for each of the encountered v2_connection_nodes
		- Sometimes cross_section_locations end up at start or end vertex of the channel. Perhaps this happens when the split point is at a vertex where a cross section location is?
EXAMPLE:

	SELECT 	SplitChannel (chn.id, ST_Union(csp.geom))
	FROM    v2_channel AS chn
	JOIN    tmp.channel_split_points AS csp
		ON  ST_DWithin(csp.geom, chn.the_geom, 0.001)
	GROUP BY    chn.id
	;	

*/
DROP TYPE IF EXISTS SplitChannel_ReturnType CASCADE;
CREATE TYPE SplitChannel_ReturnType AS (
        new_channel integer,
	    new_start_node integer,
	    new_end_node integer,
	    new_cross_section_locations integer[]	
    )
;

DROP FUNCTION IF EXISTS splitchannel(bigint, geometry, double precision,double precision);
CREATE OR REPLACE FUNCTION SplitChannel (
	channel_id bigint,
	locations geometry,
    tolerance double precision default 0.001,
    vertex_add_dist double precision default 1.0 /*,
    new_channel OUT bigint,
	new_start_node OUT bigint,
	new_end_node OUT bigint,
	new_cross_section_locations OUT bigint[]	
*/
)
RETURNS
	setof SplitChannel_ReturnType
AS
$BODY$
	DECLARE 
		channel v2_channel%rowtype;
        channel_id_seq_currval integer;
        locations_snapped geometry;
        fractions double precision[]; 
	BEGIN
        
        SELECT * FROM v2_channel WHERE id = channel_id INTO channel;
        
        -- Split geometry
        ---- Snap cut locations within tolerance to channel 
        WITH points AS (
            SELECT (ST_Dump(locations)).geom 
        )
        SELECT ST_Collect(ST_ClosestPoint((channel).the_geom, points.geom)) INTO locations_snapped 
        FROM    points
        WHERE ST_DWithin((channel).the_geom, points.geom, tolerance)
		;
        
        ---- Calculate cut fractions and cut up de channel geometry accordingly
        ---- Add vertices just before and/or after cut location (at vertex_add_dist from cut location), this is where the cross section locations will be placed
        DROP TABLE IF EXISTS SplitChannel_Segments;
		CREATE TEMPORARY TABLE SplitChannel_Segments ON COMMIT DROP AS 
        WITH fractions AS (
            SELECT ST_LineLocatePoint((channel).the_geom, s.geom) AS frac
            FROM   ST_Dump(locations_snapped)  AS s
        ),
        linesubstrings AS (
            SELECT (ST_LineSubstrings((channel).the_geom, array_agg(frac ORDER BY frac))).geom 
            FROM    fractions
        ),
        where_has_it_been_cut AS (
            SELECT	geom,
                ST_Intersects(ST_EndPoint(lss.geom), ST_Snap(locations, lss.geom, tolerance)) AS cut_at_end,
                ST_Intersects(ST_StartPoint(lss.geom), ST_Snap(locations, lss.geom, tolerance)) AS cut_at_start
            FROM	linesubstrings AS lss
        ),    
        start_vertex_added AS (
            SELECT  CASE    WHEN    cut_at_start 
                                    AND ST_Distance(ST_PointN(geom, 1), ST_PointN(geom, 2)) > vertex_add_dist
                            THEN ST_AddPoint(geom, ST_LineInterpolatePoint(geom, vertex_add_dist/ST_Length(geom)), 1) -- ST_AddPoint uses 0-based index for vertices!!!
                            ELSE geom
                    END AS geom,
                    cut_at_end,
                    cut_at_start
            FROM    where_has_it_been_cut
        ) 
        SELECT  CASE    WHEN    cut_at_end 
                                AND ST_Distance(ST_PointN(geom, ST_NumPoints(geom)), ST_PointN(geom, ST_NumPoints(geom)-1)) > vertex_add_dist
                        THEN ST_AddPoint(geom, ST_LineInterpolatePoint(geom, 1-(vertex_add_dist/ST_Length(geom))), ST_NumPoints(geom)-1) -- ST_AddPoint uses 0-based index for vertices!!!
                        ELSE geom
                END AS geom,
                cut_at_end,
                cut_at_start
        FROM    start_vertex_added
		;
                                                               
        -- Insert extra connection nodes
		DROP TABLE IF EXISTS SplitChannel_NewConnectionNodes;
		CREATE TEMPORARY TABLE SplitChannel_NewConnectionNodes ON COMMIT DROP AS
        WITH existing_cono_locations AS (
            SELECT ST_Union(the_geom) AS geom FROM v2_connection_nodes
        ),
        cono_insert_locations AS (
             SELECT ST_StartPoint(geom) AS geom FROM SplitChannel_Segments WHERE cut_at_start AND NOT ST_Equals(ST_StartPoint(geom), ST_StartPoint((channel).the_geom))
             UNION
             SELECT ST_EndPoint(geom) AS geom FROM SplitChannel_Segments WHERE cut_at_end AND NOT ST_Equals(ST_EndPoint(geom), ST_EndPoint((channel).the_geom))
        )
        SELECT  nextval('v2_connection_nodes_id_seq') AS id, 
                NULL::double precision AS storage_area, 
                (
                    ((1-ST_LineLocatePoint((channel).the_geom, cil.geom)) * cono_start.initial_waterlevel) 
                    +
                    (ST_LineLocatePoint((channel).the_geom, cil.geom) * cono_end.initial_waterlevel)
                )::double precision AS initial_waterlevel, 
                cil.geom AS the_geom, 
                'added by SplitChannel function'::text  AS code
        FROM    cono_insert_locations AS cil
        JOIN    existing_cono_locations AS old
            ON  ST_Disjoint(old.geom, cil.geom)
        JOIN    v2_connection_nodes AS cono_start
            ON  (channel).connection_node_start_id = cono_start.id
        JOIN    v2_connection_nodes AS cono_end
            ON  (channel).connection_node_end_id = cono_end.id
        ;

		INSERT INTO v2_connection_nodes(id, storage_area, initial_waterlevel, the_geom, code)
		SELECT 	id, storage_area, initial_waterlevel, the_geom, code
		FROM	SplitChannel_NewConnectionNodes
		;
		
        --- Add vertex at vertex_add_dist from cut location if not present within this distance
        --- And add resulting geometry to v2_channel table
        DROP TABLE IF EXISTS SplitChannel_DeletedXSecLocations;
        CREATE TEMPORARY TABLE SplitChannel_DeletedXSecLocations ON COMMIT DROP AS     
        SELECT * FROM v2_cross_section_location AS xsec
        WHERE xsec.channel_id = (channel).id;
		
        DELETE FROM v2_cross_section_location AS xsec WHERE xsec.channel_id = (channel).id;
        
		DROP TABLE IF EXISTS SplitChannel_NewChannels;
		CREATE TEMPORARY TABLE SplitChannel_NewChannels ON COMMIT DROP AS
        SELECT  nextval('v2_channel_id_seq') AS id,
                (channel).display_name||'_'||row_number() over() AS display_name,
                (channel).code||'_'||row_number() over() AS code,                                                                           
                (channel).calculation_type, (channel).dist_calc_points, (channel).zoom_category, 
                chn_nw.geom AS the_geom,
                cono_start.id AS connection_node_start_id, 
                cono_end.id AS connection_node_end_id                                                                           
        FROM    SplitChannel_Segments AS chn_nw
        JOIN    v2_connection_nodes AS cono_start
            ON  ST_DWithin(ST_StartPoint(chn_nw.geom), cono_start.the_geom, 0.001)
        JOIN    v2_connection_nodes AS cono_end
            ON  ST_DWithin(ST_EndPoint(chn_nw.geom), cono_end.the_geom, 0.001)
        ;
        
		INSERT INTO v2_channel (id, display_name, code, calculation_type, dist_calc_points, zoom_category, the_geom, connection_node_start_id, connection_node_end_id)
        SELECT id, display_name, code, calculation_type, dist_calc_points, zoom_category, the_geom, connection_node_start_id, connection_node_end_id
		FROM SplitChannel_NewChannels
		;
		
        DELETE FROM v2_channel WHERE id = (channel).id;
        
	    ---- Copy cross section location to added vertex
	    	-- cross section id is taken from nearest cross section location (before the split)
	    	-- reference level is interpolated between previous and next
    		-- initial water level is interpolated between previous and next                                                                           
		DROP TABLE IF EXISTS SplitChannel_NewCrossSectionLocations;
		CREATE TEMPORARY TABLE SplitChannel_NewCrossSectionLocations ON COMMIT DROP AS
        WITH xsec_add_positions AS (
            SELECT  ST_PointN(geom, 2) AS geom FROM SplitChannel_Segments WHERE cut_at_start
            UNION
            SELECT  ST_PointN(geom, ST_NumPoints(geom)-1) AS geom FROM SplitChannel_Segments WHERE cut_at_end
        ),
        xsecs_add_positions_with_fraction AS (
            SELECT  geom, 
                    ST_LineLocatePoint((channel).the_geom, geom) AS frac 
            FROM    xsec_add_positions
        ),
        xsecs_with_fraction AS (
            SELECT  xsec.*, 
                    ST_LineLocatePoint((channel).the_geom, xsec.the_geom) AS frac 
            FROM    SplitChannel_DeletedXSecLocations AS xsec
            WHERE   xsec.channel_id = (channel).id
        ),
        nearest AS (
            SELECT  DISTINCT ON (pos.frac)
                    pos.frac,        
                    pos.geom,
                    n.id AS nearest_xsec_id
            FROM    xsecs_add_positions_with_fraction AS pos
            LEFT JOIN xsecs_with_fraction AS n
                ON  1=1
            ORDER BY pos.frac, abs(pos.frac - n.frac)
        ),
        prev AS (
            SELECT  DISTINCT ON (pos.frac)
                    pos.*,
                    prev.reference_level AS previous_reference_level,
                    prev.bank_level AS previous_bank_level,
                    abs(pos.frac - prev.frac) AS dist_to_previous
            FROM    nearest AS pos
            LEFT JOIN xsecs_with_fraction AS prev
                ON  prev.frac < pos.frac
            ORDER BY pos.frac, abs(pos.frac - prev.frac)
        ),
        nxt AS (
            SELECT  DISTINCT ON (pos.frac)
                    pos.*,        
                    nxt.reference_level AS next_reference_level,
                    nxt.bank_level AS next_bank_level,
                    abs(pos.frac - nxt.frac) AS dist_to_next
            FROM    prev AS pos
            LEFT JOIN xsecs_with_fraction AS nxt
                ON  nxt.frac > pos.frac
            ORDER BY pos.frac, abs(pos.frac - nxt.frac)
        )
        SELECT  nextval('v2_cross_section_location_id_seq') AS id,
				chn.id AS channel_id, 
                nearest_xsec.definition_id AS definition_id,
                COALESCE(
                        COALESCE(
                                previous_reference_level + (next_reference_level - previous_reference_level)*(dist_to_previous/(dist_to_previous + dist_to_next)), 
                                previous_reference_level
                        ),
                        next_reference_level
                ) AS reference_level,
                nearest_xsec.friction_type, 
                nearest_xsec.friction_value, 
                COALESCE(
                        COALESCE(
                                previous_bank_level + (next_bank_level - previous_bank_level)*(dist_to_previous/(dist_to_previous + dist_to_next)), 
                                previous_bank_level
                        ),
                        next_bank_level
                ) AS bank_level,
                nw_xsec.geom AS the_geom, 
                nearest_xsec.code||'_'||row_number() over() AS code                                       
        FROM    nxt AS nw_xsec
        JOIN    v2_channel AS chn
        ON      ST_DWithin(chn.the_geom, nw_xsec.geom, 0.001)
        JOIN    SplitChannel_DeletedXSecLocations AS nearest_xsec
        ON      nw_xsec.nearest_xsec_id = nearest_xsec.id
        ;
        
        INSERT INTO v2_cross_section_location(id, channel_id, definition_id, reference_level, friction_type, friction_value, bank_level, the_geom, code)
		SELECT 	id, nw_xsec.channel_id, definition_id, reference_level, friction_type, friction_value, bank_level, the_geom, code
		FROM	SplitChannel_NewCrossSectionLocations AS nw_xsec
		;
	
		-- Insert the original cross section locations with the proper channel_id
        INSERT INTO v2_cross_section_location(channel_id, definition_id, reference_level, friction_type, friction_value, bank_level, the_geom, code)
        SELECT  chn.id AS channel_id, 
                ori_xsec.definition_id, 
                ori_xsec.reference_level,
                ori_xsec.friction_type, 
                ori_xsec.friction_value, 
                ori_xsec.bank_level,
                ST_Snap(ori_xsec.the_geom, chn.the_geom, 0.001) AS the_geom,
                ori_xsec.code                                       
        FROM    SplitChannel_DeletedXSecLocations AS ori_xsec
        JOIN    v2_channel AS chn		
		ON      ST_DWithin(chn.the_geom, ori_xsec.the_geom, 0.001)
		;

		RETURN QUERY
		SELECT chn.id::integer, nw_start_node.id::integer, nw_end_node.id::integer, array_agg(xsec.id::integer)
		FROM	SplitChannel_NewChannels AS chn
		LEFT JOIN	SplitChannel_NewConnectionNodes AS nw_start_node
			ON	chn.connection_node_start_id = nw_start_node.id
		LEFT JOIN	SplitChannel_NewConnectionNodes AS nw_end_node
			ON	chn.connection_node_end_id = nw_end_node.id
		LEFT JOIN	SplitChannel_NewCrossSectionLocations AS xsec
			ON 	xsec.channel_id = chn.id
		GROUP BY 	chn.id, nw_start_node.id, nw_end_node.id
		;
		
	END;
$BODY$ LANGUAGE plpgsql;
