-------------------------------------------------------------------
-- SPLIT PIPE BASED ON FRACTION
-- INPUT:	- v2_model
-- 		    - pipe_id, fraction and interpolation (true/false)
-- OUTPUT:	- duplicates pipe and interpolates invert levels
-- 		    - insert new connection_node
-------------------------------------------------------------------
DROP FUNCTION IF EXISTS SplitPipe(integer,double precision,boolean);
CREATE OR REPLACE FUNCTION SplitPipe (
	split_id integer,
    fraction double precision,
    interpolate boolean default true,
    new_pipe OUT integer,
	new_start_node OUT integer
)
RETURNS
	setof record
AS
$BODY$
    BEGIN
        DROP TABLE IF EXISTS result_split_pipe;
		CREATE TEMPORARY TABLE result_split_pipe ON COMMIT DROP AS
        with insert_connection_node as (
            INSERT INTO v2_connection_nodes(storage_area, initial_waterlevel, the_geom, code)
		    SELECT storage_area, initial_waterlevel, ST_Line_Interpolate_Point(p.the_geom, fraction), cs.code || '_2'
            FROM v2_pipe_view p
            JOIN v2_connection_nodes cs ON p.pipe_connection_node_start_id = cs.id
            WHERE pipe_id = split_id
            RETURNING * ),
        insert_manhole as (
            INSERT INTO v2_manhole(
            display_name, code, connection_node_id, shape, width, length,
            manhole_indicator, calculation_type, bottom_level, surface_level,
            drain_level, zoom_category)
            SELECT
                ms.display_name || '_2' as display_name,
                ms.code || '_2' as code,
                cs.id as connection_node_id,
                ms.shape,
                ms.width,
                ms.length,
                ms.manhole_indicator,
                ms.calculation_type,
                ms.bottom_level,           -- to be interpolated
                ms.surface_level,          -- to be interpolated
                ms.drain_level,            -- to be interpolated
                ms.zoom_category
			FROM insert_connection_node cs, v2_manhole ms
			JOIN v2_pipe p ON p.connection_node_start_id = ms.connection_node_id
			WHERE p.id = split_id
            RETURNING *
        ),
		duplicate_pipe AS (
			INSERT INTO v2_pipe(
				display_name, code, sewerage_type, 
				invert_level_start_point, invert_level_end_point, cross_section_definition_id, 
				material, original_length, zoom_category, connection_node_start_id, connection_node_end_id)
			SELECT 
				p.display_name || '_2', 
				p.code || '_2', 
				p.sewerage_type, 
				p.invert_level_start_point, 
				p.invert_level_end_point, 
				p.cross_section_definition_id, 
				p.material, 
				p.original_length, 
				p.zoom_category,  
				p.connection_node_start_id,
				icn.id
			FROM v2_pipe p, insert_connection_node icn
			WHERE p.id = split_id
			RETURNING *
		),update_current_pipe AS (
			UPDATE v2_pipe
			SET connection_node_start_id = cs.id
			FROM insert_connection_node cs
			WHERE v2_pipe.id = split_id
			RETURNING *
		)
        select ip.id as pipe_id, icn.id as connection_node_id 
		from duplicate_pipe ip, insert_connection_node icn;
		
    RETURN QUERY
		SELECT pipe_id, connection_node_id FROM result_split_pipe;
    END;
$BODY$ LANGUAGE plpgsql;