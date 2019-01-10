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
- SQL snippet(s) demonstrating how to use the function

*/


CREATE OR REPLACE FUNCTION <functionname> (
	<name_arg1> <type_arg1>,
	<name_arg2> <type_arg2>,
	<name_argn> <type_argn>
)
RETURNS
	<type_output>
AS
$BODY$
	DECLARE 
		<name_internal_variable1> <type_internal_variable1>;
		<name_internal_variable2> <type_internal_variable2>;
		<name_internal_variablen> <type_internal_variablen>;
	BEGIN
		RETURN <value>;
	END;
$BODY$ LANGUAGE plpgsql;