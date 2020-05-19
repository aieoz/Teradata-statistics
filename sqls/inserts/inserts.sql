MERGE INTO $SYSTEM_DATABASE_NAME.inserts as inserts USING
(

	SELECT '$DAY' as t_measure_date,
			'$TABLE_NAME' as t_table_name,
			'$DATABASE_NAME' as t_database_name,
			CASE 
					WHEN CAST('$DAY' AS DATE)<CURRENT_TIMESTAMP THEN 1
			ELSE 0
					END AS t_complete,
			
			ZEROIFNULL(s_insert_single) AS t_insert_single,
			ZEROIFNULL(s_insert_group) AS t_insert_group

	FROM (
			SELECT
				SUM(IsInsert) AS s_insert_single,
                SUM(HasInsert) AS s_insert_group
			FROM v_inserts GROUP BY ObjectTableName
	) AS volatile_reference RIGHT OUTER JOIN 
	(
		SELECT 0 as fullview
	) AS operation_types ON 1=1

) AS t_
ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name

WHEN MATCHED THEN
	UPDATE SET complete=t_complete, insert_single = t_insert_single, insert_group = t_insert_group

WHEN NOT MATCHED THEN
	INSERT (measure_date, database_name, table_name, complete, insert_single, insert_group)
	VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_insert_single, t_insert_group);
