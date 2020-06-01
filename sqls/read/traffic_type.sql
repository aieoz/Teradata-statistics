
SELECT 
CAST((period_begin (FORMAT 'YYYY-mm-dd')) AS VARCHAR(10)), 
CAST((period_end (FORMAT 'YYYY-mm-dd')) AS VARCHAR(10)),
scope,
Selects,
Inserts,
Updates,
Deletes,
InsSels
FROM 
(
    SELECT CAST('$END' AS DATE) - (bgn * PERIOD_GROUP) AS period_end, 
    CAST('$END' AS DATE) - (bgn * (PERIOD_GROUP + 1)) AS period_begin,
    (CAST('$END' AS DATE)- MEASURE_DATE) / bgn AS PERIOD_GROUP, 
    GREATEST((CAST('$END' AS DATE) - CAST('$BEGIN' AS DATE)) / 10, 1) AS bgn,
    scope,
    SUM(SelectOption) AS Selects,
    SUM(InsertOption) AS Inserts,
    SUM(UpdateOption) AS Updates,
    SUM(DeleteOption) AS Deletes,
    SUM(InsSelOption) AS InsSels

    FROM TDSP_ANALYSIS.traffic_type 
    WHERE MEASURE_DATE 
    BETWEEN CAST('$BEGIN' AS DATE) 
    AND CAST('$END' AS DATE)
    AND PERIOD_GROUP<10
    AND database_name='$DATABASE_NAME'
    AND table_name='$TABLE_NAME'
    GROUP BY period_begin, period_end, PERIOD_GROUP, scope

) AS temp;