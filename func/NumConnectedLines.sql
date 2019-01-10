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
	SELECT NumConnections('v2_connection_nodes', 23); 
	
	[2]
	-- List all connection nodes without any connections
	SELECT * FROM v2_connection_nodes WHERE NumConnections('v2_connection_nodes', id) = 0;

	[3]
	-- List all boundary conditions with <> 1 connection
	SELECT * FROM v2_boundary_conditions WHERE NumConnections('v2_connection_nodes', connection_node_id) != 1;
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
