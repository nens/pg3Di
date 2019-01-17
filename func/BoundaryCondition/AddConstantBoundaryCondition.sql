-------------------------------------------------------------------
-- DELETE PIPE
-- INPUT:	- v2_model
-- 		    - pipe_id
-- OUTPUT:	- delete pipe
-------------------------------------------------------------------
DROP FUNCTION IF EXISTS AddConstantBoundaryCondition(INTEGER, DOUBLE PRECISION, INTEGER);
CREATE
	OR REPLACE FUNCTION AddConstantBoundaryCondition (
	boundary_connection_node_id INTEGER,
	constant_waterlevel DOUBLE PRECISION,
    boundary_type INTEGER default 1
	)
RETURNS void AS $BODY$

BEGIN
	INSERT INTO v2_1d_boundary_conditions (
		connection_node_id,
		boundary_type,
		timeseries
		)
	SELECT boundary_connection_node_id,
		boundary_type,
		E'0,' || constant_waterlevel || E'\n99999,' || constant_waterlevel AS timeseries;
        -- update v2_manhole if available
    UPDATE v2_manhole SET manhole_indicator = 1 WHERE v2_manhole.connection_node_id = boundary_connection_node_id;
END;$BODY$

LANGUAGE plpgsql;
