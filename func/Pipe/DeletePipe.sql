-------------------------------------------------------------------
-- DELETE PIPE
-- INPUT:	- v2_model
-- 		    - pipe_id
-- OUTPUT:	- delete pipe
-------------------------------------------------------------------
DROP FUNCTION IF EXISTS DeletePipe(integer);
CREATE OR REPLACE FUNCTION DeletePipe (
	delete_id integer
)
RETURNS
	void
AS
$BODY$
    BEGIN
        DELETE FROM v2_pipe WHERE id = delete_id;
    END;
$BODY$ LANGUAGE plpgsql;