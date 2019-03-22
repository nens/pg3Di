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
  
*/