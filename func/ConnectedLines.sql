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
	BEGIN
		-- checks on inputs
		IF object_type NOT IN ('v2_connection_nodes')
		THEN RAISE EXCEPTION '% is not a valid object_type or functionality for % has not yet been implemented', object_type, object_type;
		END IF;
		
		IF side NOT IN ('start'::varchar(5), 'end'::varchar(5), 'both'::varchar(5)) 
		THEN RAISE EXCEPTION '% is not a valid string for side argument', side USING HINT = 'Choose from start, end or both.';
		END IF;
		
		IF object_type = 'v2_connection_nodes'
		THEN
		RETURN QUERY
			-- pumpstation
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_pumpstation'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'start'::varchar(5)::varchar(5) AS connected_object_side 
			FROM 	v2_pumpstation 
			WHERE 	connection_node_start_id = object_id
			UNION
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_pumpstation'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_pumpstation 
			WHERE 	connection_node_end_id = object_id
			UNION
			
			-- pipe
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_pipe'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_pipe 
			WHERE 	connection_node_start_id = object_id
			UNION
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_pipe'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_pipe 
			WHERE 	connection_node_end_id = object_id
			UNION
			
			-- weir
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_weir'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_weir 
			WHERE 	connection_node_start_id = object_id
			UNION
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_weir'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_weir 
			WHERE 	connection_node_end_id = object_id
			UNION
			
			-- orifice
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_orifice'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_orifice 
			WHERE 	connection_node_start_id = object_id
			UNION
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_orifice'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_orifice 
			WHERE 	connection_node_end_id = object_id
			UNION
			
			-- culvert
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_culvert'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_culvert 
			WHERE 	connection_node_start_id = object_id
			UNION
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_culvert'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'end'::varchar(5) AS connected_object_side 
			FROM 	v2_culvert 
			WHERE 	connection_node_end_id = object_id
			UNION
			
			-- channel
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_channel'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'start'::varchar(5) AS connected_object_side 
			FROM 	v2_channel 
			WHERE 	connection_node_start_id = object_id
			UNION
			SELECT 	NULL::varchar(5) AS input_side, 
					'v2_channel'::varchar(64) AS connected_object_type, 
					connection_node_start_id AS connected_object_id, 
					'end'::varchar(5)  AS connected_object_side 
			FROM 	v2_channel 
			WHERE 	connection_node_end_id = object_id
			;
		END IF;
	END;
$BODY$ LANGUAGE plpgsql;




