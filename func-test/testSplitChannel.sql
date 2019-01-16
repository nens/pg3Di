-- Reset test data
    --CREATE TABLE tmp.test_v2_cross_section_location AS SELECT * FROM v2_cross_section_location WHERE id IN (166, 167);
    --CREATE TABLE tmp.test_v2_connection_nodes AS SELECT * FROM v2_connection_nodes WHERE id IN (47,48);
    --SELECT * FROM tmp.test_v2_channel;
    --UPDATE tmp.test_v2_cross_section_location SET channel_id = (SELECT id FROM tmp.test_v2_channel);
    DELETE FROM v2_channel CASCADE;
    DELETE FROM v2_connection_nodes;
    DELETE FROM v2_cross_section_location;
    INSERT INTO v2_connection_nodes SELECT * FROM tmp.test_v2_connection_nodes;
    INSERT INTO v2_channel SELECT * FROM tmp.test_v2_channel;
    INSERT INTO v2_cross_section_location SELECT * FROM tmp.test_v2_cross_section_location;

-- Without overview of inserted objects
    SELECT 	SplitChannel(chn.id, ST_Union(csp.geom))
    FROM    v2_channel AS chn
    JOIN    tmp.test_split_locations AS csp
        ON  ST_DWithin(csp.geom, chn.the_geom, 0.001)
    GROUP BY    chn.id
    ;

-- With overview of inserted objects
    WITH actual_split AS (
        SELECT 	chn.id AS old_channel, 
                SplitChannel(chn.id, ST_Union(csp.geom)) AS nw
    	FROM    v2_channel AS chn
    	JOIN    tmp.test_split_locations AS csp
    		ON  ST_DWithin(csp.geom, chn.the_geom, 0.001)
    	GROUP BY    chn.id
    )
    SELECT  old_channel, 
            (nw).* 
    FROM actual_split 
    ORDER BY old_channel, new_channel;                             
	;