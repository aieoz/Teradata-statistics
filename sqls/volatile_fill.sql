DELETE FROM v_join;

INSERT INTO v_join (QueryID, 
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
	 WHEN Statements<=1 THEN 1 
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
   END AS l_DeleteOption

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