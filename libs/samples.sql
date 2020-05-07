-- Days from beggining
SELECT calendar_date FROM Sys_Calendar.Calendar 
WHERE calendar_date<=CURRENT_TIMESTAMP AND calendar_date>=cast ('2020-01-01' as DATE);