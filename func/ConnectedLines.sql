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
