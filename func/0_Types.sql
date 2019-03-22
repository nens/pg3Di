
-- pg3Di_LineObjectString
    DROP TYPE IF EXISTS pg3Di_LineObjectString CASCADE;
	CREATE TYPE pg3Di_LineObjectString AS 
		enum('v2_channel', 'v2_culvert', 'v2_weir', 'v2_orifice', 'v2_pumpstation', 'v2_pipe')
	;