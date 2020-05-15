
SELECT 
CAST((period_begin (FORMAT 'YYYY-mm-dd')) AS VARCHAR(10)), 
CAST((period_end (FORMAT 'YYYY-mm-dd')) AS VARCHAR(10)),
statement_group,
total
FROM 
(
    SELECT CAST('$END' AS DATE) - (bgn * PERIOD_GROUP) AS period_end, 
    CAST('$END' AS DATE) - (bgn * (PERIOD_GROUP + 1)) AS period_begin,
    (CAST('$END' AS DATE)- MEASURE_DATE) / bgn AS PERIOD_GROUP, 
    (CAST('$END' AS DATE) - CAST('$BEGIN' AS DATE)) / 10 AS bgn,
    statement_group,
    SUM(total) AS total

    FROM $SYSTEM_DATABASE_NAME.traffic_type 
    WHERE MEASURE_DATE 
    BETWEEN CAST('$BEGIN' AS DATE) 
    AND CAST('$END' AS DATE)
    AND PERIOD_GROUP<10
    AND database_name='$DATABASE_NAME'
    AND table_name='$TABLE_NAME'
    GROUP BY period_begin, period_end, PERIOD_GROUP, statement_group

) AS temp;