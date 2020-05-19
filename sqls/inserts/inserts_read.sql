
SELECT 
    measure_date,
    insert_single,
    insert_group

    FROM $SYSTEM_DATABASE_NAME.inserts
    WHERE MEASURE_DATE 
    BETWEEN CAST('$BEGIN' AS DATE) 
    AND CAST('$END' AS DATE)
    AND database_name='$DATABASE_NAME'
    AND table_name='$TABLE_NAME'
    ORDER BY measure_date