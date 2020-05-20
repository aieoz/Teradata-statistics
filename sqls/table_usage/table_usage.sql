MERGE INTO $SYSTEM_DATABASE_NAME.table_usage as table_usage USING
(

	SELECT 
	'$DAY' AS t_measure_date,
	'$TABLE_NAME' AS t_table_name,
	'$DATABASE_NAME' AS t_database_name,
	CASE 
		WHEN CAST('$DAY' AS DATE)<CURRENT_TIMESTAMP THEN 1
	ELSE 0
		END AS t_complete,
	ZEROIFNULL(COUNT(*)) AS t_uses_total
	FROM DBC.DBQLObjTbl
	WHERE LOWER(ObjectTableName)=LOWER('$DATABASE_NAME')
	AND ObjectType='Tab'
	AND LOWER(ObjectDatabaseName)=LOWER('$TABLE_NAME')
	AND CollectTimeStamp=CAST('$DAY' AS DATE)

) AS t_
ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name

WHEN MATCHED THEN
	UPDATE SET complete=t_complete, uses_total=t_uses_total

WHEN NOT MATCHED THEN
	INSERT (measure_date, database_name, table_name, complete, uses_total)
	VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_uses_total);
