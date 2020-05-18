CREATE VOLATILE TABLE v_join 
(
        QueryID BIGINT,
        CollectTimeStamp TIMESTAMP,
        ObjectTableName VARCHAR(100),
        scope BYTEINT,
        SelectOption INT,
        InsertOption INT,
        UpdateOption INT,
        DeleteOption INT,
        InsSelOption INT
)
ON COMMIT PRESERVE ROWS;