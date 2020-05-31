CREATE MACRO TDSP_ANALYSIS.collect_tables_usage
(
       database_name 	VARCHAR(128),
       table_name 		VARCHAR(128),
       col_day  		DATE FORMAT 'YYYY-MM-DD'
)
AS
(
    MERGE INTO TDSP_ANALYSIS.table_usage as table_usage USING
    (

        SELECT 
        :col_day AS t_measure_date,
        :table_name AS t_table_name,
        :database_name AS t_database_name,
        CASE 
            WHEN CAST(:col_day AS DATE)<CURRENT_TIMESTAMP THEN 1
        ELSE 0
            END AS t_complete,
        ZEROIFNULL(COUNT(*)) AS t_uses_total
        FROM DBC.DBQLObjTbl
        WHERE LOWER(ObjectTableName)=LOWER(:database_name)
        AND ObjectType='Tab'
        AND LOWER(ObjectDatabaseName)=LOWER(:table_name)
        AND CollectTimeStamp=:col_day

    ) AS t_
    ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name

    WHEN MATCHED THEN
        UPDATE SET complete=t_complete, uses_total=t_uses_total

    WHEN NOT MATCHED THEN
        INSERT (measure_date, database_name, table_name, complete, uses_total)
        VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_uses_total);
);
