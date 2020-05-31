CREATE VOLATILE TABLE v_traffic_type
(
        QueryID BIGINT,
        CollectTimeStamp TIMESTAMP,
        ObjectTableName VARCHAR(128),
        scope BYTEINT,
        SelectOption INT,
        InsertOption INT,
        UpdateOption INT,
        DeleteOption INT,
        InsSelOption INT
)
ON COMMIT PRESERVE ROWS;

CREATE MACRO TDSP_ANALYSIS.collect_traffic_type_create_volatile
AS
(
    ---------------------------------
    ----- CREATE VOLATILE TABLE -----
    ---------------------------------
    CREATE VOLATILE TABLE v_traffic_type
    (
            QueryID BIGINT,
            CollectTimeStamp TIMESTAMP,
            ObjectTableName VARCHAR(128),
            scope BYTEINT,
            SelectOption INT,
            InsertOption INT,
            UpdateOption INT,
            DeleteOption INT,
            InsSelOption INT
    )
    ON COMMIT PRESERVE ROWS;

);

CREATE MACRO TDSP_ANALYSIS.collect_traffic_type_drop_volatile
AS
(
    DROP TABLE v_traffic_type;
);

CREATE MACRO TDSP_ANALYSIS.collect_traffic_type
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
    INSERT INTO v_traffic_type (QueryID, 
		CollectTimeStamp,
		ObjectTablename,
		scope,
		SelectOption, 
		InsertOption,
		UpdateOption,
        DeleteOption,
        InsSelOption
	)

    SELECT l_QueryID,
        CollectTimeStamp as l_CollectTimeStamp,
        ObjectTableName AS l_ObjectTableName,
    CASE 
        WHEN (l_SelectOption + l_InsertOption + l_UpdateOption + l_DeleteOption + l_InsSelOption)=1 THEN 1 
        ELSE 0 
        END AS l_Statements, 
    -- Select option
    CASE  
        WHEN Statements=0 THEN 0 
        WHEN Statements=1 THEN 
        CASE
            WHEN StatementGroup LIKE '%Select%'
                THEN 1
                ELSE 0
            END
            ELSE
                CASE 
                    WHEN StatementGroup LIKE 'DML%'
                    THEN CAST(REGEXP_REPLACE(StatementGroup, '^.*Sel=', '') AS INT)
            ELSE 0
        END
    END AS l_SelectOption,
    -- InsertOption
    CASE  
        WHEN Statements=0 THEN 0 
        WHEN Statements=1 THEN 
        CASE
            WHEN StatementGroup LIKE '%Insert%'
                THEN 1
                ELSE 0
            END
            ELSE
                CASE 
                    WHEN StatementGroup LIKE 'DML%'
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(StatementGroup, '^.*Ins=', ''), ' .*', '') AS INT)
            ELSE 0
        END
    END AS l_InsertOption,
    -- UpdateOption
    CASE  
        WHEN Statements=0 THEN 0 
        WHEN Statements=1 THEN 
        CASE
            WHEN StatementGroup LIKE '%Update%'
                THEN 1
                ELSE 0
            END
            ELSE
                CASE 
                    WHEN StatementGroup LIKE 'DML%'
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(StatementGroup, '^.*Upd=', ''), ' .*', '') AS INT)
            ELSE 0
        END
    END AS l_UpdateOption,
    -- DeleteOption
    CASE  
        WHEN Statements=0 THEN 0 
        WHEN Statements=1 THEN 
        CASE
            WHEN StatementGroup LIKE '%Delete%'
                THEN 1
                ELSE 0
            END
            ELSE
                CASE 
                    WHEN StatementGroup LIKE 'DML%'
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(StatementGroup, '^.*Del=', ''), ' .*', '') AS INT)
            ELSE 0
        END
    END AS l_DeleteOption,
    -- InsSelOption
    CASE  
        WHEN Statements=0 THEN 0 
        WHEN Statements=1 THEN 
        CASE
            WHEN StatementGroup LIKE '%Delete%'
                THEN 1
                ELSE 0
            END
            ELSE
                CASE 
                    WHEN StatementGroup LIKE 'DML%'
                    THEN CAST(REGEXP_REPLACE(REGEXP_REPLACE(StatementGroup, '^.*Del=', ''), ' .*', '') AS INT)
            ELSE 0
        END
    END AS l_InsSelOption

        FROM 
        (

            SELECT QueryID AS l_QueryID,
                CollectTimeStamp, 
                StatementType,
                Statements,
                StatementGroup
            FROM DBC.DBQLogTbl 
            WHERE CollectTimeStamp=:col_day

        ) AS logs 
        JOIN
        (

            SELECT QueryID AS o_QueryID, ObjectTableName, ObjectDatabaseName FROM DBC.DBQLObjTbl 
            WHERE 
            LOWER(ObjectTableName)=LOWER(:table_name)
            AND LOWER(ObjectDatabaseName)=LOWER(:database_name)
            AND CollectTimeStamp=:col_day

        )AS objsts
    ON o_QueryID=l_QueryID;

    ---------------------------------
    -----    FILL MAIN TABLE    -----
    ---------------------------------
    MERGE INTO TDSP_ANALYSIS.traffic_type as traffic_type USING
    (

        SELECT :col_day as t_measure_date,
                :table_name as t_table_name,
                :database_name as t_database_name,
                CASE 
                        WHEN :col_day<CURRENT_TIMESTAMP THEN 1
                ELSE 0
                        END AS t_complete,
                ZEROIFNULL(scope) as t_scope,
                ZEROIFNULL(t_SelectOption) as t_SelectOption,
                ZEROIFNULL(t_InsertOption) as t_InsertOption,
                ZEROIFNULL(t_UpdateOption) as t_UpdateOption,
                ZEROIFNULL(t_DeleteOption) as t_DeleteOption,
                ZEROIFNULL(t_InsSelOption) as t_InsSelOption
        FROM (
                SELECT scope as t_scope,
                    SUM(SelectOption) AS t_SelectOption, 
                    SUM(InsertOption) AS t_InsertOption, 
                    SUM(UpdateOption) AS t_UpdateOption, 
                    SUM(DeleteOption) AS t_DeleteOption, 
                    SUM(InsSelOption) AS t_InsSelOption
                FROM v_traffic_type GROUP BY scope
        ) AS volatile_reference RIGHT OUTER JOIN 
        (
            SELECT scope FROM 
            (
                SELECT * FROM (SELECT 0 as scope) x
                union all
                SELECT * FROM (SELECT 1 as scope) x
            ) as scopes
        ) AS operation_types ON scope=t_scope

    ) AS t_
    ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name AND scope=t_scope

    WHEN MATCHED THEN
        UPDATE SET complete=t_complete, SelectOption=t_SelectOption, InsertOption=t_InsertOption, UpdateOption=t_UpdateOption, DeleteOption=t_DeleteOption, InsSelOption=t_InsSelOption

    WHEN NOT MATCHED THEN
        INSERT (measure_date, database_name, table_name, complete, scope, SelectOption, InsertOption, UpdateOption, DeleteOption, InsSelOption)
        VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_scope, t_SelectOption, t_InsertOption, t_UpdateOption, t_DeleteOption, t_InsSelOption);


);

DROP TABLE v_traffic_type;