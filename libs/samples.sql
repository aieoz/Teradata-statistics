-- Days from begginingc
SELECT calendar_date FROM Sys_Calendar.Calendar 
WHERE calendar_date<=cast('2020-05-13' as DATE) AND calendar_date>=cast('2020-04-13' as DATE);

-- Missing days
SELECT calendar_date FROM Sys_Calendar.Calendar AS CALENDAR
LEFT OUTER JOIN 
(SELECT * FROM TDSP_ANALYSIS.daily_usage) AS MEASURES 
ON calendar_date = measure_date
WHERE calendar_date<=cast('2020-05-13' as DATE) 
AND calendar_date>=cast('2020-04-13' as DATE)
AND
(complete=0 OR complete IS NULL);

-- Tables daily
SELECT CAST('2020-04-14' AS DATE) AS measure_date, 
 SYSHOURS.HOUR_ID as measure_hour, 
 'MOVIES' as table_name, 
 CASE 
  	WHEN CAST('2020-04-14' AS DATE)<CURRENT_TIMESTAMP THEN 1
 ELSE 0
 END AS complete,
 ZEROIFNULL(LOGS.TOTAL) as uses_total
 FROM TDSP_ANALYSIS.system_hours AS SYSHOURS 
LEFT OUTER JOIN 
(
	SELECT EXTRACT(HOUR FROM CollectTimeStamp)
	AS "DHOUR", COUNT(QueryID) AS TOTAL FROM DBC.DBQLObjTbl 
	GROUP BY DHOUR
	WHERE ObjectTableName='MOVIES' 
	AND ObjectDatabaseName='movies'
	AND CollectTimeStamp=CAST('2020-04-14' AS DATE)
) AS LOGS ON DHOUR = HOUR_ID;