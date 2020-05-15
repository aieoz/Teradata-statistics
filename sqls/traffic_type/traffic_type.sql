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
	SELECT 
			StatementGroup,
			COUNT(*) AS t_total
			FROM 
			(
					SELECT QueryID, StatementGroup
					FROM DBC.DBQLogTbl 
					WHERE CollectTimeStamp=cast('$DAY' as DATE)
			) AS logs 

			JOIN 

			(
					SELECT QueryID, CollectTimeStamp, ObjectTableName
					FROM DBC.DBQLObjTbl 
					WHERE ObjectType='Tab' 
					AND LOWER(ObjectTableName)=LOWER('$TABLE_NAME')
					AND LOWER(ObjectDatabaseName)=LOWER('$DATABASE_NAME')
					AND CollectTimeStamp=cast('$DAY' as DATE)
			) AS objlogs ON objlogs.QueryID=logs.QueryID

			GROUP BY StatementGroup
	) AS ssss RIGHT OUTER JOIN 
	(
		SELECT traffic_type from (
		select traffic_type from (select 'Select' as traffic_type) x
			union all
			select * from (select 'Update' as traffic_type) x
			union all
			select * from (select 'Insert' as traffic_type) x
		) AS lbls
	) AS sdasda ON traffic_type=StatementGroup


) AS t_
ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name AND statement_group=t_statement_group

WHEN MATCHED THEN
	UPDATE SET complete=t_complete, total=t_total

WHEN NOT MATCHED THEN
	INSERT (measure_date, database_name, table_name, complete, statement_group, total)
	VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_statement_group, t_total);
