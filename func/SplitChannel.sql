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
