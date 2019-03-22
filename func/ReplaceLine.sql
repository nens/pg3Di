/*

DESCRIPTION: 
It replaces a line object with another line object
The attributes of the new object will be chosen using this hierarchy:
    -- 1. Your input (values in the object_to_insert argument)
    -- 2. Object to replace (e.g. if the connection_node_start_id in object_to_insert argument is NULL, then the connection_node_start_id of the original line object is used)
    -- 3. Default values in the object_to_insert 3Di table

INPUTS:
- 	object_type_to_replace: pg3Di_LineObjectString (one of: 'v2_channel', 'v2_culvert', 'v2_weir', 'v2_orifice', 'v2_pumpstation', 'v2_pipe')
-	object_id_to_replace. The id of the object to be removed
-	object_to_insert. pg3Di_LineObjectString (one of: 'v2_channel', 'v2_culvert', 'v2_weir', 'v2_orifice', 'v2_pumpstation', 'v2_pipe')

OUTPUTS: 
-   The function returns the id of the object that is inserted
-   If the object to replace is not found, it raises an exception
-	Returns NULL if any inputs are NULL
-   The function deletes the object to replace, no backup of this object is saved

DEPENDENCIES:
-	pg3Di
-	hstore

REMARKS: 
- If you replace a Pipe by an Orifice or Weir, 'sewerage' will be set to TRUE, unless explicitly set to FALSE in the insert_attributes
- If you replace a Channel, its Cross Section Locations will be deleted also 

EXAMPLE(S):
	-- New v2_culvert inherits all matching fields from v2_channel:
	SELECT ReplaceLine('v2_channel', 23, 'v2_culvert')

	-- New v2_culvert does not inherit any values from v2_channel, except the_geom, connection_node_start_id, connection_node_end_id (all other fields will be NULL):
	SELECT ReplaceLine('v2_channel', 23, 'v2_culvert', FALSE)
	
	-- New v2_culvert inherits all matching fields from v2_channel, except the invert_level_start_point and invert_level_end_point
	SELECT ReplaceLine('v2_channel', 23, 'v2_culvert', 'invert_level_start_point=>12.2, invert_level_end_point=>11.59') 

	-- New v2_orifice inherits all matching fields from v2_pipe, sewerage is set to True automatically, change discharge_coefficient_negative in the conversion
    SELECT ReplaceLine('v2_pipe', 1214, 'v2_orifice', 'discharge_coefficient_negative=>0.0');	    

*/

CREATE OR REPLACE FUNCTION ReplaceLine (
	object_type_to_replace pg3Di_LineObjectString,
	object_id_to_replace integer,
	insert_type pg3Di_LineObjectString,
	insert_attributes hstore default ''::hstore,
	inherit boolean default TRUE
)
RETURNS
	integer
AS
$BODY$
	DECLARE 
        nr_objects_to_replace integer;
--		object_to_insert record;
		object_to_insert hstore;
        object_to_replace hstore;
       	att_types hstore; 
		default_values hstore;
		tgt_values_string TEXT;
		values_to_insert_string text;
	BEGIN
	
		IF nr_objects_to_replace != 1
        THEN    RAISE NOTICE 'No % with id % exists. Cannot replace.', object_type_to_replace, object_id_to_replace;
                RETURN NULL;
        END IF;	
		
		EXECUTE format('SELECT count(*) FROM %I WHERE id = %s;', object_type_to_replace, object_id_to_replace) INTO nr_objects_to_replace;
        
        IF nr_objects_to_replace != 1
        THEN    RAISE NOTICE 'No % with id % exists. Cannot replace.', object_type_to_replace, object_id_to_replace;
                RETURN NULL;
        END IF;

		-- Make hstore of object to replace, delete null-keys from this hstore (hstore(record))
        EXECUTE format('SELECT hstore(obj) FROM (SELECT * FROM %1$I WHERE id = %2$s) AS obj;', object_type_to_replace::text::regclass, object_id_to_replace) INTO object_to_replace;
       
		-- Prevent that the pk is copied from the object_to_replace
		object_to_replace := object_to_replace - 'id'::text;
		
		-- Make hstore of non-NULL default values of object to insert
		-- Only include defaults for field that are not given as input in insert_attributes; this also prevents...
		-- ...unnecessarily affecting any sequences, e.g. by calling nextval('v2_orifice_id_seq') when in the end the default for id is not used 
		WITH arrays AS (	
			SELECT  array_agg(a.attname ORDER BY a.attnum) AS attname,
					array_agg(t.typname ORDER BY a.attnum) AS typname, -- !! Not needed if we use this somewhere further down: EXECUTE format('INSERT INTO public.%1$I SELECT ((%2$s)::%1$I).*', insert_type, string_agg(svals(hstore(object_to_insert)), ',')) and cast it to a insert_type
					array_agg(
						eval(
							CASE WHEN insert_attributes ? a.attname THEN NULL  	-- Don't evaluate the default if it will not be used
							ELSE pg_get_expr(									-- Get the expression of the default if it will be used
									d.adbin, 
									d.adrelid
								)												
							END
						) ORDER BY a.attnum
					) AS defaultval

			FROM    pg_catalog.pg_attribute a
			LEFT JOIN pg_catalog.pg_type AS t
				ON a.atttypid = t.oid
			LEFT JOIN pg_catalog.pg_attrdef d 
				ON  (a.attrelid, a.attnum) = (d.adrelid,  d.adnum)
			WHERE   NOT    	a.attisdropped   -- no dropped (dead) columns
					AND    	a.attnum > 0         -- no system columns
					AND    	a.attrelid = ('public.'||insert_type)::regclass
		)
		SELECT 	hstore(attname, typname), hstore(attname, defaultval)
		FROM	arrays
		INTO 	att_types, default_values
		;

		-- Make empty hstore of the object to insert
        --EXECUTE format('SELECT NULL::%I;', insert_type::text) INTO object_to_insert;
        EXECUTE format('SELECT hstore(NULL::%I);', insert_type::text) INTO object_to_insert;

		-- Load default values into object_to_insert
		object_to_insert := update_hstore(object_to_insert, DeleteNullKeys(default_values));

		-- Set sewerage to true if a pipe is replaced
		IF object_type_to_replace = 'v2_pipe' THEN object_to_insert := update_hstore(object_to_insert, 'sewerage=>true'::hstore); END IF;
	
		-- Replace defaults with matching values from object_to_replace
		IF inherit THEN object_to_insert := update_hstore(object_to_insert, DeleteNullKeys(object_to_replace)); END IF;
	
		-- Replace fields now in object_to_insert record with matching values from insert_attributes hstore
		-- Allow the user to explicitly overrule defaults or inherited values with NULL values, so no DeleteNullKeys here
		object_to_insert := update_hstore(object_to_insert, insert_attributes);

        -- Remove cross section locations if object_type_to_replace = v2_channel
        -- !! We should actually fix this defining the FK of v2_channel with ON DELETE CASCADE
        IF      object_type_to_replace = 'v2_channel'
        THEN    EXECUTE format('DELETE FROM v2_cross_section_location WHERE channel_id = %s;', object_id_to_replace);
        END IF;
        
        -- Delete the object to be replaced
        EXECUTE format('DELETE FROM %I WHERE id = %s;', object_type_to_replace, object_id_to_replace);
		
		-- Insert the object to be inserted
		--	-- Make a string that looks like " '23'::integer, 'River rhine'::text etc " to paste after 'INSERT INTO {} VALUES '
		WITH 	attvals_table AS (SELECT (each(object_to_insert)).*),
				atttypes_table AS (SELECT (each(att_types)).*)
		SELECT	string_agg(t.key, ','),
				REPLACE(string_agg(''''||COALESCE(v.value, 'NULL')||'''::'||t.value, ','), '''NULL''', 'NULL')
		INTO	tgt_values_string, values_to_insert_string
		FROM	atttypes_table AS t 
		LEFT JOIN attvals_table AS v
		ON 		v.key = t.key
		;

		-- -- Do the actual insertion
		EXECUTE format('INSERT INTO public.%I (%s) VALUES (%s);', insert_type, tgt_values_string, values_to_insert_string);
        
        RETURN (object_to_insert -> 'id')::integer;
	END;
$BODY$ 
LANGUAGE plpgsql 
RETURNS NULL ON NULL INPUT
;
