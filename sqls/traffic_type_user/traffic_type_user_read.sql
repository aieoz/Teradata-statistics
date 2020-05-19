SELECT statement_type, user_id, user_name, sum(total) AS uses FROM $SYSTEM_DATABASE_NAME.traffic_type_user
WHERE measure_date BETWEEN CAST('$BEGIN' AS DATE) AND CAST('$END' AS DATE)
AND database_name='$DATABASE_NAME'
AND table_name='$TABLE_NAME'
AND statement_type is not null
GROUP BY statement_type, user_id, user_name;