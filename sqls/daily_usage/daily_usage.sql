MERGE INTO $SYSTEM_DATABASE_NAME.daily_usage as daily_usage USING
(

	SELECT CAST('$DAY' AS DATE) AS t_measure_date, 
	 SYSHOURS.HOUR_ID as t_measure_hour,
	 '$TABLE_NAME' as t_table_name, 
     '$DATABASE_NAME' as t_database_name,
	 CASE 
	  	WHEN CAST('$DAY' AS DATE)<CURRENT_TIMESTAMP THEN 1
	 ELSE 0
	 END AS t_complete,
	 ZEROIFNULL(LOGS.TOTAL) as t_uses_total
	 FROM $SYSTEM_DATABASE_NAME.system_hours AS SYSHOURS 
	LEFT OUTER JOIN 
	(
		SELECT EXTRACT(HOUR FROM CollectTimeStamp)
		AS "DHOUR", COUNT(QueryID) AS TOTAL FROM DBC.DBQLObjTbl 
		GROUP BY DHOUR
		WHERE LOWER(ObjectTableName)=LOWER('$DATABASE_NAME')
		AND ObjectType='Tab'
		AND LOWER(ObjectDatabaseName)=LOWER('$TABLE_NAME')
		AND CollectTimeStamp=CAST('$DAY' AS DATE)
	) AS LOGS ON DHOUR = HOUR_ID

) AS t_
ON measure_date=t_measure_date AND measure_hour=t_measure_hour AND table_name=t_table_name AND database_name=t_database_name

WHEN MATCHED THEN
	UPDATE SET complete=t_complete, uses_total=t_uses_total

WHEN NOT MATCHED THEN
	INSERT (measure_date, measure_hour, database_name, table_name, complete, uses_total)
	VALUES (t_measure_date, t_measure_hour, t_database_name, t_table_name, t_complete, t_uses_total);
