SELECT DISTINCT CAST((calendar_date (FORMAT 'YYYY-mm-dd')) AS VARCHAR(10)) FROM Sys_Calendar.Calendar AS CALENDAR
LEFT OUTER JOIN 
(SELECT * FROM $DATABASE_NAME.daily_usage) AS MEASURES 
ON calendar_date = measure_date
WHERE calendar_date<=cast('$END' as DATE) 
AND calendar_date>=cast('$BEGIN' as DATE)
AND
(complete=0 OR complete IS NULL);