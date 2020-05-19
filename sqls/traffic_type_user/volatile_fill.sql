DELETE FROM v_traffic_type_user;

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