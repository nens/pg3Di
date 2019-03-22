--- Eval
	--- Made by Andreas Dietrich https://stackoverflow.com/questions/7433201/are-there-any-way-to-execute-a-query-inside-the-string-value-like-eval-in-post 
	create or replace function eval( sql  text ) 
	returns text as $$
	declare
	  as_txt  text;
	begin
	  if  sql is null  then  return null ;  end if ;
	  execute  'SELECT '|| sql  into  as_txt ;
	  return  as_txt ;
	end;
	$$ language plpgsql;

-- update_hstore
	DROP FUNCTION IF EXISTS update_hstore(hstore, hstore);
	CREATE FUNCTION update_hstore(hstore, hstore) RETURNS hstore
		AS 'select delete($1, akeys($2))||slice($1, akeys($2));'
		LANGUAGE SQL
		IMMUTABLE STRICT
	;	
	
-- DeleteNullKeys
	CREATE OR REPLACE FUNCTION DeleteNullKeys (input_hstore hstore)
	RETURNS
		hstore
	AS
	$BODY$
		DECLARE 
			i text;
			output_hstore hstore;
		BEGIN
			output_hstore := input_hstore;
			FOR i IN (SELECT skeys(input_hstore))
			LOOP 
				IF NOT 	defined(input_hstore, i)
				THEN 	output_hstore := delete(output_hstore, i);
				END IF;
            END LOOP;
			RETURN output_hstore;
		END;
	$BODY$ LANGUAGE plpgsql;

