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