CREATE VOLATILE TABLE v_inserts
(
        QueryID BIGINT,
        CollectTimeStamp TIMESTAMP,
        ObjectTableName VARCHAR(128),
        IsInsert BYTEINT,
        HasInsert BYTEINT
) ON COMMIT PRESERVE ROWS;