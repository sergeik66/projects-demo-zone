SELECT 
 s.name  AS [ObjectSchema] 
, a.name AS [ObjectName]
, (SELECT SUM(sdmvPTNS.row_count) 
	FROM sys.objects AS sOBJ
      INNER JOIN sys.dm_db_partition_stats AS sdmvPTNS
            ON sOBJ.object_id = sdmvPTNS.object_id
			WHERE sOBJ.name = a.name AND sOBJ.is_ms_shipped = 0x0
			 AND sdmvPTNS.index_id < 2 
			) AS [EstimatedRowCount]
,(STUFF((SELECT ', ' + ku.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc  
	INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku on tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
	WHERE ku.TABLE_SCHEMA = s.name AND ku.TABLE_NAME = a.name
    GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME,ku.COLUMN_NAME
	FOR XML PATH('')), 1, 2, '')) AS  CandidateKey
FROM sys.objects a
INNER JOIN sys.schemas s
ON a.schema_id = s.schema_id
where a.type IN ('U','V')
GROUP BY s.name, a.name

/* fields metadata */
SELECT
e.name AS [ObjectSchema]
,a.name AS [ObjectName]
,d.ORDINAL_POSITION AS [ColumnOrdinal]
,c.name AS [FieldName]
,CONCAT('[', c.name, ']') AS [SourceQueryPart]
,CAST(c.is_nullable AS int) AS [Nullable]
,CASE 
	     WHEN d.Data_Type in ('decimal','numeric') then d.Data_Type + '(' + CONVERT(varchar,c.precision) + ',' + CONVERT(varchar,c.scale) + ')'
         WHEN d.Data_Type in ('date','datetime','datetime2','smalldatetime') then 'datetime2(7)'
         WHEN d.Data_Type in ('nvarchar','nchar') and c.max_length > 0 then d.Data_Type + '(' + CONVERT(varchar,c.max_length/2) + ')'
         WHEN d.Data_Type in ('nvarchar','nchar') and c.max_length = -1 then d.Data_Type + '(4000)'
         WHEN d.Data_Type in ('varchar','char') and c.max_length > 0 then d.Data_Type + '(' + CONVERT(varchar,c.max_length) + ')'
         WHEN d.Data_Type in ('varchar','char') and c.max_length = -1 then d.Data_Type + '(8000)'
         WHEN d.Data_Type = 'uniqueidentifier' then 'char(36)'
         WHEN d.Data_Type = 'text' then 'varchar(8000)'
         WHEN d.Data_Type = 'ntext' then 'nvarchar(4000)'
     --    WHEN d.Data_Type = 'varbinary' then 'varbinary(' + CONVERT(varchar,iif(c.max_length > 8000 or c.max_length < 1,8000,c.max_length)) + ')'   
    --     WHEN d.Data_Type = 'binary' then 'binary(' + CONVERT(varchar,iif(c.max_length > 8000 or c.max_length < 1,8000,c.max_length)) + ')'   
         WHEN d.Data_Type = 'xml' then 'varchar(8000)'
       ELSE d.Data_Type
	   END AS [FieldType]
FROM sys.tables a
INNER JOIN sys.columns c
ON a.object_id = c.object_id 
INNER JOIN sys.schemas e
ON a.schema_id = e.schema_id
INNER JOIN INFORMATION_SCHEMA.COLUMNS d
ON d.COLUMN_NAME = c.name
AND d.TABLE_NAME = a.name
AND d.TABLE_SCHEMA = e.name
UNION ALL
SELECT
e.name AS [ObjectSchema]
,a.name AS [ObjectName]
,d.ORDINAL_POSITION AS [ColumnOrdinal]
,c.name AS [FieldName]
,CONCAT('[', c.name, ']') AS [SourceQueryPart]
,c.is_nullable AS [Nullable]
,CASE 
	     WHEN d.Data_Type in ('decimal','numeric') then d.Data_Type + '(' + CONVERT(varchar,c.precision) + ',' + CONVERT(varchar,c.scale) + ')'
         WHEN d.Data_Type in ('date','datetime','datetime2','smalldatetime') then 'datetime2(7)'
         WHEN d.Data_Type in ('nvarchar','nchar') and c.max_length > 0 then d.Data_Type + '(' + CONVERT(varchar,c.max_length/2) + ')'
         WHEN d.Data_Type in ('nvarchar','nchar') and c.max_length = -1 then d.Data_Type + '(4000)'
         WHEN d.Data_Type in ('varchar','char') and c.max_length > 0 then d.Data_Type + '(' + CONVERT(varchar,c.max_length) + ')'
         WHEN d.Data_Type in ('varchar','char') and c.max_length = -1 then d.Data_Type + '(8000)'
         WHEN d.Data_Type = 'uniqueidentifier' then 'char(36)'
         WHEN d.Data_Type = 'text' then 'varchar(8000)'
         WHEN d.Data_Type = 'ntext' then 'nvarchar(4000)'
     --    WHEN d.Data_Type = 'varbinary' then 'varbinary(' + CONVERT(varchar,iif(c.max_length > 8000 or c.max_length < 1,8000,c.max_length)) + ')'   
     --    WHEN d.Data_Type = 'binary' then 'binary(' + CONVERT(varchar,iif(c.max_length > 8000 or c.max_length < 1,8000,c.max_length)) + ')'   
         WHEN d.Data_Type = 'xml' then 'varchar(8000)'
       ELSE d.Data_Type
	   END AS [FieldType]
FROM sys.views a
INNER JOIN sys.columns c
ON a.object_id = c.object_id 
INNER JOIN sys.schemas e
ON a.schema_id = e.schema_id
INNER JOIN INFORMATION_SCHEMA.COLUMNS d
ON d.COLUMN_NAME = c.name
AND d.TABLE_NAME = a.name
AND d.TABLE_SCHEMA = e.name
ORDER BY 1,2,3

