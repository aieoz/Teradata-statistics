MERGE INTO $SYSTEM_DATABASE_NAME.traffic_type as traffic_type USING
(

	SELECT '$DAY' as t_measure_date,
			'$TABLE_NAME' as t_table_name,
			'$DATABASE_NAME' as t_database_name,
			CASE 
					WHEN CAST('$DAY' AS DATE)<CURRENT_TIMESTAMP THEN 1
			ELSE 0
					END AS t_complete,
			ZEROIFNULL(scope) as t_scope,
			ZEROIFNULL(t_SelectOption) as t_SelectOption,
			ZEROIFNULL(t_InsertOption) as t_InsertOption,
			ZEROIFNULL(t_UpdateOption) as t_UpdateOption,
			ZEROIFNULL(t_DeleteOption) as t_DeleteOption,
			ZEROIFNULL(t_InsSelOption) as t_InsSelOption
	FROM (
			SELECT scope as t_scope,
				SUM(SelectOption) AS t_SelectOption, 
				SUM(InsertOption) AS t_InsertOption, 
				SUM(UpdateOption) AS t_UpdateOption, 
				SUM(DeleteOption) AS t_DeleteOption, 
				SUM(InsSelOption) AS t_InsSelOption
			FROM v_traffic_type GROUP BY scope
	) AS volatile_reference RIGHT OUTER JOIN 
	(
		SELECT scope FROM 
		(
			SELECT * FROM (SELECT 0 as scope) x
			union all
			SELECT * FROM (SELECT 1 as scope) x
		) as scopes
	) AS operation_types ON scope=t_scope

) AS t_
ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name AND scope=t_scope

WHEN MATCHED THEN
	UPDATE SET complete=t_complete, SelectOption=t_SelectOption, InsertOption=t_InsertOption, UpdateOption=t_UpdateOption, DeleteOption=t_DeleteOption, InsSelOption=t_InsSelOption

WHEN NOT MATCHED THEN
	INSERT (measure_date, database_name, table_name, complete, scope, SelectOption, InsertOption, UpdateOption, DeleteOption, InsSelOption)
	VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_scope, t_SelectOption, t_InsertOption, t_UpdateOption, t_DeleteOption, t_InsSelOption);
