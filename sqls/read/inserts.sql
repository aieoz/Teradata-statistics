
SELECT 
measure_date,
scope,
SUM(InsertOption) AS Inserts

FROM TDSP_ANALYSIS.traffic_type 
WHERE MEASURE_DATE 
BETWEEN CAST('$BEGIN' AS DATE) 
AND CAST('$END' AS DATE)
AND database_name='$DATABASE_NAME'
AND table_name='$TABLE_NAME'
GROUP BY measure_date, scope
ORDER BY measure_date;
