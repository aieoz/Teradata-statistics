CREATE VOLATILE TABLE v_join 
(
        QueryID BIGINT,
        CollectTimeStamp TIMESTAMP,
        StatementGroup VARCHAR(40),
        ObjectTableName VARCHAR(100)
)
ON COMMIT PRESERVE ROWS;