SELECT measure_hour,
sum(uses_total) as uses_total, 
AVG(uses_total) as avg_uses, 
MAX(uses_total) as max_uses 
FROM $SYSTEM_DATABASE_NAME.daily_usage 
GROUP BY measure_hour
WHERE table_name='$TABLE_NAME'
AND database_name='$DATABASE_NAME'
AND measure_date BETWEEN CAST('$BEGIN' AS DATE) AND CAST('$END' AS DATE)
ORDER BY measure_hour;