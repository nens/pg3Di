CREATE TABLE tmp.test_v2_cross_section_location AS SELECT * FROM v2_cross_section_location WHERE id IN (166, 167);
CREATE TABLE tmp.test_v2_connection_nodes AS SELECT * FROM v2_connection_nodes WHERE id IN (47,48);
SELECT * FROM tmp.test_v2_channel;
UPDATE tmp.test_v2_cross_section_location SET channel_id = (SELECT id FROM tmp.test_v2_channel);
DELETE FROM v2_channel CASCADE;
DELETE FROM v2_connection_nodes;
INSERT INTO v2_connection_nodes SELECT * FROM tmp.test_v2_connection_nodes;
INSERT INTO v2_channel SELECT * FROM tmp.test_v2_channel;
INSERT INTO v2_cross_section_location SELECT * FROM tmp.test_v2_cross_section_location;


    SELECT 	SplitChannel (chn.id, ST_Union(csp.geom))
	FROM    v2_channel AS chn
	JOIN    tmp.channel_split_points AS csp
		ON  ST_DWithin(csp.geom, chn.the_geom, 0.001)
	GROUP BY    chn.id
	;	