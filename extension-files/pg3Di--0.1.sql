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




/*

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

EXAMPLE:

	SELECT 	SplitChannel (chn.id, ST_Union(csp.geom))
	FROM    v2_channel AS chn
	JOIN    tmp.channel_split_points AS csp
		ON  ST_DWithin(csp.geom, chn.the_geom, 0.001)
	GROUP BY    chn.id
	;	

*/
DROP FUNCTION IF EXISTS splitchannel(integer,geometry,double precision,double precision);
CREATE OR REPLACE FUNCTION SplitChannel (
	channel_id integer,
	locations geometry,
    tolerance double precision default 0.001,
    vertex_add_dist double precision default 1.0,
    new_channel OUT integer,
	new_start_node OUT integer,
	new_end_node OUT integer,
	new_cross_section_locations OUT integer[]	

)
RETURNS
	setof record
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
                NULL AS storage_area, 
                ((1-ST_LineLocatePoint((channel).the_geom, cil.geom)) * cono_start.initial_waterlevel) 
                    +
                (ST_LineLocatePoint((channel).the_geom, cil.geom) * cono_end.initial_waterlevel)
                    AS initial_waterlevel, 
                cil.geom AS the_geom, 
                'added by SplitChannel function'  AS code
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
		SELECT 	id, channel_id, definition_id, reference_level, friction_type, friction_value, bank_level, the_geom, code
		FROM	SplitChannel_NewCrossSectionLocations
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
		SELECT chn.id, nw_start_node.id, nw_end_node.id, array_agg(xsec.id)
		FROM	SplitChannel_NewChannels AS chn
		LEFT JOIN	SplitChannel_NewConnectionNodes AS nw_start_node
			ON	chn.connection_node_start_id = nw_start_node.id
		LEFT JOIN	SplitChannel_NewConnectionNodes AS nw_end_node
			ON	chn.connection_node_end_id = nw_end_node.id
		LEFT JOIN	SplitChannel_NewCrossSectionLocations AS xsec
			ON 	xsec.channel_id = chn.id
		GROUP BY 	chn.id
		;
		
	END;
$BODY$ LANGUAGE plpgsql;
