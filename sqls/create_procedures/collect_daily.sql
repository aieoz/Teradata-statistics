CREATE MACRO TDSP_ANALYSIS.collect_daily_usage
(
       database_name 	VARCHAR(128),
       table_name 		VARCHAR(128),
       col_day  		DATE FORMAT 'YYYY-MM-DD'
)
AS
(

	MERGE INTO TDSP_ANALYSIS.daily_usage as daily_usage USING
	(

		SELECT :col_day AS t_measure_date, 
		 SYSHOURS.HOUR_ID as t_measure_hour,
		 :table_name as t_table_name, 
	     :database_name as t_database_name,
		 CASE 
		  	WHEN :col_day<CURRENT_TIMESTAMP THEN 1
		 ELSE 0
		 END AS t_complete,
		 ZEROIFNULL(LOGS.TOTAL) as t_uses_total
		 FROM TDSP_ANALYSIS.system_hours AS SYSHOURS 
		LEFT OUTER JOIN 
		(
			SELECT EXTRACT(HOUR FROM CollectTimeStamp)
			AS "DHOUR", COUNT(QueryID) AS TOTAL FROM DBC.DBQLObjTbl 
			GROUP BY DHOUR
			WHERE LOWER(ObjectTableName)=LOWER(:database_name)
			AND ObjectType='Tab'
			AND LOWER(ObjectDatabaseName)=LOWER(:table_name)
			AND CollectTimeStamp=:col_day
		) AS LOGS ON DHOUR = HOUR_ID
	
	) AS t_
	ON measure_date=t_measure_date AND measure_hour=t_measure_hour AND table_name=t_table_name AND database_name=t_database_name
	
	WHEN MATCHED THEN
		UPDATE SET complete=t_complete, uses_total=t_uses_total
	
	WHEN NOT MATCHED THEN
		INSERT (measure_date, measure_hour, database_name, table_name, complete, uses_total)
		VALUES (t_measure_date, t_measure_hour, t_database_name, t_table_name, t_complete, t_uses_total);
);
