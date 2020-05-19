CREATE VOLATILE TABLE v_traffic_type_user
(
        QueryID BIGINT,
        CollectTimeStamp TIMESTAMP,
        ObjectTableName VARCHAR(128),
        UserID BYTE(4),
        UserName VARCHAR(128),
        StatementType VARCHAR(20)
)
ON COMMIT PRESERVE ROWS;