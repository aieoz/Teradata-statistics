-----------------------------------------
------- CREATE DATABASE AND TABLES ------
-----------------------------------------
CREATE DATABASE TDSP_ANALYSIS FROM DBC AS PERM = 10000000000; -- 10 GB
CREATE TABLE TDSP_ANALYSIS.daily_usage (
    measure_date    DATE NOT NULL,
    measure_hour    INT,
    table_name      VARCHAR(128) NOT NULL,
    database_name   VARCHAR(128) NOT NULL,
    complete        BYTEINT,
    uses_total      DECIMAL(12, 2)
);
CREATE TABLE TDSP_ANALYSIS.table_usage (
    measure_date    DATE NOT NULL,
    table_name      VARCHAR(128) NOT NULL,
    database_name   VARCHAR(128) NOT NULL,
    complete        BYTEINT,
    uses_total      DECIMAL(12, 2)
);
CREATE TABLE TDSP_ANALYSIS.traffic_type (
    measure_date    DATE NOT NULL,
    table_name      VARCHAR(128) NOT NULL,
    database_name   VARCHAR(128) NOT NULL,
    complete        BYTEINT,
    scope           BYTEINT,
    SelectOption INT,
    InsertOption INT,
    UpdateOption INT,
    DeleteOption INT,
    InsSelOption INT
);
CREATE TABLE TDSP_ANALYSIS.traffic_type_user (
    measure_date    DATE NOT NULL,
    table_name      VARCHAR(128) NOT NULL,
    database_name   VARCHAR(128) NOT NULL,
    complete        BYTEINT,
    statement_type  VARCHAR(20),
    num_of          INT,
    user_id         BYTE(4),
    user_name       VARCHAR(128),
    total           INT
);
CREATE TABLE TDSP_ANALYSIS.inserts (
    measure_date    DATE NOT NULL,
    table_name      VARCHAR(128) NOT NULL,
    database_name   VARCHAR(128) NOT NULL,
    complete        BYTEINT,
    insert_single   BIGINT,
    insert_group    BIGINT
);
CREATE TABLE TDSP_ANALYSIS.system_hours (
    HOUR_ID DECIMAL(2,0)
);

---------------------------
------- FILL TABLES -------
---------------------------
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (0);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (1);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (2);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (3);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (4);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (5);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (6);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (7);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (8);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (9);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (10);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (11);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (12);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (13);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (14);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (15);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (16);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (17);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (18);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (19);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (20);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (21);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (22);
INSERT INTO TDSP_ANALYSIS.system_hours (HOUR_ID) VALUES (23);

---------------------------
------- PERMISSIONS -------
---------------------------
-- GRANT CREATE PROCEDURE
-- ON TDSP_ANALYSIS
-- TO dbc;

            ---------------------------
            --- CREATE PROCEDURES -----
            ---------------------------

    -------------------------------------------
    -------------  COLLECT DAILY  -------------
    ------------------------------------------- 
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

    -------------------------------------------
    -------------  COLLECT TABLES -------------
    -------------------------------------------
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

    -------------------------------------------
    ------------- COLLECT INSERTS -------------
    -------------------------------------------
CREATE VOLATILE TABLE v_inserts
(
        QueryID BIGINT,
        CollectTimeStamp TIMESTAMP,
        ObjectTableName VARCHAR(128),
        IsInsert BYTEINT,
        HasInsert BYTEINT
) ON COMMIT PRESERVE ROWS;

CREATE MACRO TDSP_ANALYSIS.collect_inserts_create_volatile
AS
(
    ---------------------------------
    ----- CREATE VOLATILE TABLE -----
    ---------------------------------
    CREATE VOLATILE TABLE v_inserts
    (
            QueryID BIGINT,
            CollectTimeStamp TIMESTAMP,
            ObjectTableName VARCHAR(128),
            IsInsert BYTEINT,
            HasInsert BYTEINT
    ) ON COMMIT PRESERVE ROWS;

);

CREATE MACRO TDSP_ANALYSIS.collect_inserts_drop_volatile
AS
(
    DROP TABLE v_inserts;
);

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
DROP TABLE v_inserts;

    -------------------------------------------
    ---------- COLLECT TRAFFIC TYPE -----------
    -------------------------------------------
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
DROP TABLE v_traffic_type;


    -------------------------------------------
    -----  COLLECT TRAFFIC TYPE AND USER ------
    -------------------------------------------
CREATE VOLATILE TABLE v_traffic_type_user
(
        QueryID BIGINT,
        CollectTimeStamp TIMESTAMP,
        ObjectTableName VARCHAR(128),
        UserID BYTE(4),
        UserName VARCHAR(128),
        StatementType VARCHAR(20)
) ON COMMIT PRESERVE ROWS;

CREATE MACRO TDSP_ANALYSIS.collect_traffic_type_user_create_volatile
AS
(
    ---------------------------------
    ----- CREATE VOLATILE TABLE -----
    ---------------------------------
    CREATE VOLATILE TABLE v_traffic_type_user
    (
            QueryID BIGINT,
            CollectTimeStamp TIMESTAMP,
            ObjectTableName VARCHAR(128),
            UserID BYTE(4),
            UserName VARCHAR(128),
            StatementType VARCHAR(20)
    ) ON COMMIT PRESERVE ROWS;

);

CREATE MACRO TDSP_ANALYSIS.collect_traffic_type_user_drop_volatile
AS
(
    DROP TABLE v_traffic_type_user;
);

CREATE MACRO TDSP_ANALYSIS.collect_traffic_type_user
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
    INSERT INTO v_traffic_type_user (
        QueryID, 
        CollectTimeStamp,
        ObjectTablename,
        UserID,
        UserName,
        StatementType
    )

    SELECT l_QueryID,
        CollectTimeStamp as l_CollectTimeStamp,
        ObjectTableName AS l_ObjectTableName,
        UserID AS l_UserID,
        UserName AS l_UserName,
        StatementType AS l_StatementType FROM 
        (

            SELECT QueryID AS l_QueryID,
                CollectTimeStamp, 
                UserID,
                UserName,
                StatementType
            FROM DBC.DBQLogTbl 
            WHERE CollectTimeStamp=:col_day

        ) AS logs 
        JOIN
        (

            SELECT QueryID AS o_QueryID, ObjectTableName, ObjectDatabaseName FROM DBC.DBQLObjTbl 
            WHERE 
            LOWER(ObjectTableName)=LOWER('$TABLE_NAME')
            AND LOWER(ObjectDatabaseName)=LOWER('$DATABASE_NAME')
            AND CollectTimeStamp=:col_day

        )AS objsts
    ON o_QueryID=l_QueryID;

    ---------------------------------
    -----    FILL MAIN TABLE    -----
    ---------------------------------

    MERGE INTO TDSP_ANALYSIS.traffic_type_user as traffic_type_user USING
    (

        SELECT t_measure_date,
                :table_name as t_table_name,
                :database_name as t_database_name,
                CASE 
                        WHEN :col_day<CURRENT_TIMESTAMP THEN 1
                ELSE 0
                        END AS t_complete,
                t_row_num as t_num_of,
                StatementType AS t_statement_type,
                user_id     AS t_user_id,
                user_name   AS t_user_name,
                ZEROIFNULL(t_total) as t_total
        FROM (

                SELECT 
                    ROW_NUMBER() OVER (ORDER BY t_total DESC NULLS LAST) AS t_row_num,
                    StatementType,
                    userName as user_name,
                    userID as user_id,
                    COUNT(QueryID) AS t_total 

                FROM v_traffic_type_user 
                GROUP BY StatementType, user_id, user_name

        ) AS volatile_reference 
        
        RIGHT OUTER JOIN 

        (
            SELECT '$DAY' AS t_measure_date
        ) AS operation_types ON t_measure_date=t_measure_date


    ) AS t_
    ON measure_date=t_measure_date AND  table_name=t_table_name AND database_name=t_database_name AND statement_type=t_statement_type AND num_of=t_num_of

    WHEN MATCHED THEN
        UPDATE SET complete=t_complete, total=t_total, user_id=t_user_id, user_name=t_user_name

    WHEN NOT MATCHED THEN
        INSERT (measure_date, database_name, table_name, complete, num_of, statement_type, user_id, user_name, total)
        VALUES (t_measure_date, t_database_name, t_table_name, t_complete, t_num_of, t_statement_type, t_user_id, t_user_name, t_total);

);
DROP TABLE v_traffic_type_user;

-- MACRO TDSP_ANALYSIS.collect_daily_usage;
-- DROP MACRO TDSP_ANALYSIS.collect_tables_usage;
-- DROP MACRO TDSP_ANALYSIS.collect_inserts;
-- DROP MACRO TDSP_ANALYSIS.collect_inserts_create_volatile;
-- DROP MACRO TDSP_ANALYSIS.collect_inserts_action;
-- DROP MACRO TDSP_ANALYSIS.collect_inserts_drop_volatile;
-- DROP MACRO TDSP_ANALYSIS.collect_traffic_type_create_volatile;
-- DROP MACRO TDSP_ANALYSIS.collect_traffic_type_drop_volatile;
-- DROP MACRO TDSP_ANALYSIS.collect_traffic_type;
-- DROP MACRO TDSP_ANALYSIS.collect_traffic_type_user_create_volatile;
-- DROP MACRO TDSP_ANALYSIS.collect_traffic_type_user_drop_volatile;
-- DROP  MACRO TDSP_ANALYSIS.collect_traffic_type_user;