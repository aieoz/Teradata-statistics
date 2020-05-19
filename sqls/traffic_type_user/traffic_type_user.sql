MERGE INTO $SYSTEM_DATABASE_NAME.traffic_type_user as traffic_type_user USING
(

	SELECT t_measure_date,
			'$TABLE_NAME' as t_table_name,
			'$DATABASE_NAME' as t_database_name,
			CASE 
					WHEN CAST('$DAY' AS DATE)<CURRENT_TIMESTAMP THEN 1
			ELSE 0
					END AS t_complete,
            t_row_num as t_num_of,
			StatementType AS t_statement_type,
            user_id     AS t_user_id,
			user_name   AS t_user_name,
            ZEROIFNULL(t_total) as t_total
	FROM (

			SELECT 
                ROW_NUMBER() OVER (ORDER BY t_total DESC NULLS LAST) AS t_row_num,
                StatementType,
                userName as user_name,
                userID as user_id,
                COUNT(QueryID) AS t_total 

            FROM v_traffic_type_user 
            GROUP BY StatementType, user_id, user_name

	) AS volatile_reference 
    
    RIGHT OUTER JOIN 

	(
		SELECT '$DAY' AS t_measure_date
	) AS operation_types ON t_measure_date=t_measure_date


) AS t_
ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name AND statement_type=t_statement_type AND num_of=t_num_of

WHEN MATCHED THEN
	UPDATE SET complete=t_complete, total=t_total, user_id=t_user_id, user_name=t_user_name

WHEN NOT MATCHED THEN
	INSERT (measure_date, database_name, table_name, complete, num_of, statement_type, user_id, user_name, total)
	VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_num_of, t_statement_type, t_user_id, t_user_name, t_total);