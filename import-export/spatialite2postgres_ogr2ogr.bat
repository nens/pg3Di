@rem THIS .bat copies all data from a 3Di spatialite to a 3Di postgis database
@rem The target database must have the same tables as the 3Di database, but empty
@rem The source and target databases must have the same datamodel (version)

@rem spatialite
	@set spatialite_fn="C:\3Di\heathrow\heathrow_1D_Rivers_and_Structures_connection_node_debug.sqlite"

@rem postgis 3di data
	@set host=nens-3di-db-03.nens.local
	@set port=5432
	@set database=work_t0296_heathrow_edit
	@set username=threedi
	@set PGPASSWORD=1S418lTYWLFsYxud4don

for %%f in (
	v2_connection_nodes,
	v2_surface_parameters,
	v2_surface,
	v2_surface_map,
	v2_grid_refinement,
	v2_grid_refinement_area,
	v2_manhole,
	v2_channel,
	v2_cross_section_definition,
	v2_cross_section_location,
	v2_1d_lateral,
	v2_pipe,
	v2_impervious_surface,
	v2_impervious_surface_map,
	v2_orifice,
	v2_pumpstation,
	v2_pumped_drainage_area,
	v2_culvert,
	v2_2d_lateral,
	v2_initial_waterlevel,
	v2_floodfill,
	v2_weir,	
	v2_1d_boundary_conditions,
	v2_2d_boundary_conditions,
	v2_windshielding,
	v2_levee,
	v2_calculation_point,
	v2_groundwater,
	v2_simple_infiltration,
	v2_interflow,
	v2_numerical_settings,
	v2_global_settings,
	v2_aggregation_settings,
	v2_connected_pnt,
	v2_control_group,
	v2_obstacle,
	v2_dem_average_area,
	v2_control_delta,
	v2_control_timed,
	v2_control_table,
	v2_control,
	v2_control_measure_group,
	v2_control_pid,
	v2_control_measure_map,
	v2_control_memory
) do (
	echo %%f
	ogr2ogr -append -preserve_fid -f "PostgreSQL" PG:"host=%host% user=%username% dbname=%database% password=%PGPASSWORD% port=5432" %spatialite_fn% %%f -nln %%f -t_srs EPSG:28992
)
pause
