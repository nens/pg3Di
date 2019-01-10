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
