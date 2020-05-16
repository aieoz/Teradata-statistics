MERGE INTO $SYSTEM_DATABASE_NAME.traffic_type as traffic_type USING
(

	SELECT '$DAY' as t_measure_date,
			'$TABLE_NAME' as t_table_name,
			'$DATABASE_NAME' as t_database_name,
			CASE 
					WHEN CAST('$DAY' AS DATE)<CURRENT_TIMESTAMP THEN 1
			ELSE 0
					END AS t_complete,
			traffic_type AS t_statement_group,
			ZEROIFNULL(t_total) as t_total
	FROM (
			SELECT StatementGroup, 
				COUNT(QueryID) AS t_total 
			FROM v_join GROUP BY StatementGroup
	) AS volatile_reference RIGHT OUTER JOIN 
	(
		SELECT traffic_type from (
		select traffic_type from (select 'Select' as traffic_type) x
			union all
			select * from (select 'Update' as traffic_type) x
			union all
			select * from (select 'Insert' as traffic_type) x
		) AS lbls
	) AS operation_types ON traffic_type=StatementGroup


) AS t_
ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name AND statement_group=t_statement_group

WHEN MATCHED THEN
	UPDATE SET complete=t_complete, total=t_total

WHEN NOT MATCHED THEN
	INSERT (measure_date, database_name, table_name, complete, statement_group, total)
	VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_statement_group, t_total);
