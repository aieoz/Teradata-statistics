CREATE DATABASE TDSP_ANALYSIS FROM DBC;



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

CREATE TABLE TDSP_ANALYSIS.system_hours (
    HOUR_ID DECIMAL(2,0)
);
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