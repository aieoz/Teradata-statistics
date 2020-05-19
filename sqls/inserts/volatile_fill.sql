DELETE FROM v_inserts;

INSERT INTO v_inserts (QueryID, 
		CollectTimeStamp,
		ObjectTablename,
		IsInsert,
		HasInsert
	)

SELECT l_QueryID,
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