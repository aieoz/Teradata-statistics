SELECT DISTINCT CAST((calendar_date (FORMAT 'YYYY-mm-dd')) AS VARCHAR(10)) FROM Sys_Calendar.Calendar AS CALENDAR
LEFT OUTER JOIN 
(
    SELECT * FROM TDSP_ANALYSIS.$SYSTEM_TABLE_NAME WHERE
    table_name='$TABLE_NAME'
    AND database_name='$DATABASE_NAME'
) AS MEASURES 
ON calendar_date = measure_date
WHERE calendar_date<=cast('$END' as DATE) 
AND calendar_date>=cast('$BEGIN' as DATE)
AND
(complete=0 OR complete IS NULL);