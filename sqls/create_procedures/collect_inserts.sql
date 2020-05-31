
CREATE VOLATILE TABLE dbc.v_inserts
(
        QueryID BIGINT,
        CollectTimeStamp TIMESTAMP,
        ObjectTableName VARCHAR(128),
        IsInsert BYTEINT,
        HasInsert BYTEINT
) ON COMMIT PRESERVE ROWS;

CREATE MACRO TDSP_ANALYSIS.collect_inserts
(
       database_name 	VARCHAR(128),
       table_name 		VARCHAR(128),
       col_day  		DATE FORMAT 'YYYY-MM-DD'
)
AS
(

    ---------------------------------
    -----  FILL VOLATILE TABLE  -----
    ---------------------------------
    INSERT INTO dbc.v_inserts
    (
        QueryID, 
        CollectTimeStamp,
        ObjectTablename,
        IsInsert,
        HasInsert
    ) SELECT l_QueryID,
        CollectTimeStamp as l_CollectTimeStamp,
        ObjectTableName AS l_ObjectTableName,
        -- Is insert
        CASE  
            WHEN Statements=1 THEN 
            CASE
            WHEN StatementGroup LIKE '%Insert%'
            THEN 1
            ELSE 0
            END
            ELSE 0
        END AS IsInsert,
    
        -- Has insert
        CASE  
            WHEN Statements>1 THEN
            CASE 
            WHEN StatementGroup LIKE 'DML%'
            THEN 
                CASE 
                WHEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(StatementGroup, '^.*Ins=', ''), ' .*', '') AS INT) > 1
                THEN 1
                ELSE 0
                END
            ELSE 0
            END
            ELSE 0
        END AS HasInsert
        
        FROM 
        (
            SELECT QueryID AS l_QueryID,
                CollectTimeStamp, 
                StatementType,
                Statements,
                StatementGroup
            FROM DBC.DBQLogTbl 
            WHERE CollectTimeStamp=:col_day
        ) AS logs JOIN
        (
            SELECT QueryID AS o_QueryID, ObjectTableName, ObjectDatabaseName FROM DBC.DBQLObjTbl 
            WHERE 
            LOWER(ObjectTableName)=LOWER(:table_name)
            AND LOWER(ObjectDatabaseName)=LOWER(:database_name)
            AND CollectTimeStamp=:col_day
        )
        AS objsts
    ON o_QueryID=l_QueryID;

    ---------------------------------
    -----    FILL MAIN TABLE    -----
    ---------------------------------
    MERGE INTO TDSP_ANALYSIS.inserts as inserts USING
    (

        SELECT :col_day as t_measure_date,
                :table_name as t_table_name,
                :database_name as t_database_name,
                CASE 
                        WHEN :col_day<CURRENT_TIMESTAMP THEN 1
                ELSE 0
                        END AS t_complete,

                ZEROIFNULL(s_insert_single) AS t_insert_single,
                ZEROIFNULL(s_insert_group) AS t_insert_group
        FROM (
                SELECT
                    SUM(IsInsert) AS s_insert_single,
                    SUM(HasInsert) AS s_insert_group
                FROM dbc.v_inserts GROUP BY ObjectTableName
        ) AS volatile_reference RIGHT OUTER JOIN 
        (
            SELECT 0 as fullview
        ) AS operation_types ON 1=1

    ) AS t_
    ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name

    WHEN MATCHED THEN
        UPDATE SET complete=t_complete, insert_single = t_insert_single, insert_group = t_insert_group
    WHEN NOT MATCHED THEN
        INSERT (measure_date, database_name, table_name, complete, insert_single, insert_group)
        VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_insert_single, t_insert_group);

);

DROP TABLE dbc.v_inserts;