DELETE FROM v_join;

INSERT INTO v_join (QueryID, CollectTimeStamp, StatementGroup,ObjectTablename)

SELECT l_QueryID,
	CollectTimeStamp as l_CollectTimeStamp,
	StatementType AS l_StatementType,
	ObjectTableName AS l_ObjectTableName FROM 
	(

		SELECT QueryID AS l_QueryID,
			CollectTimeStamp, 
			StatementType 
		FROM DBC.DBQLogTbl 
		WHERE CollectTimeStamp=CAST('$DAY' AS DATE)

	) AS logs 
	JOIN
	(

		SELECT QueryID AS o_QueryID, ObjectTableName, ObjectDatabaseName FROM DBC.DBQLObjTbl 
		WHERE 
		LOWER(ObjectTableName)=LOWER('$TABLE_NAME')
		AND LOWER(ObjectDatabaseName)=LOWER('$DATABASE_NAME')
		AND CollectTimeStamp=CAST('$DAY' AS DATE)

	)AS objsts
ON o_QueryID=l_QueryID;