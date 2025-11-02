SET NOCOUNT ON; 

-- Variables for process parameterization 
DECLARE @ProductionTable         SYSNAME        = N'tbProducao'; 
DECLARE @StagingTable            SYSNAME        = N'tbHomologacao'; 
DECLARE @KeyColumns              NVARCHAR(MAX)  = N'cod_simulacao,modelo'; 
DECLARE @ExceptionColumns        NVARCHAR(MAX)  = N'desconto, data_parametro'; -- Expected difference due to rule adjustment 

DECLARE @LimitColumn             NVARCHAR(MAX)  = N'data_parametro'; 
DECLARE @CheckCaseSensitivity    BIT = 1;                              -- Case sensitivity check 
DECLARE @Separator               CHAR(1) = CHAR(30);                   -- Invisible Character (Unicode record separator) 

-- Working Variables 
DECLARE @ColumnName              SYSNAME; 
DECLARE @DataType                SYSNAME; 
DECLARE @Columns                 NVARCHAR(MAX)  = N''; 
DECLARE @SQL                     NVARCHAR(MAX); 
DECLARE @DebugMode               BIT = 0;                              -- If Active, dynamic queries will be displayed


-- Variables for quantity/delta tests 
DECLARE @ProdCount               BIGINT; 
DECLARE @StagingCount            BIGINT; 
DECLARE @DeltaIncludedCount      BIGINT; 
DECLARE @DeltaExcludedCount      BIGINT; 
DECLARE @DeltaHashCount          BIGINT; 

-- Clear previous execution 
DROP TABLE IF EXISTS #ProdHash 
DROP TABLE IF EXISTS #StagingHash 
DROP TABLE IF EXISTS #Audit 


-- Create Hash table for production 
CREATE TABLE #ProdHash ( 
                             KeyColumn VARCHAR(200) 
                           , Hash VARCHAR(200) 
                           ); 

-- Create Hash table for staging 
CREATE TABLE #StagingHash    ( 
                             KeyColumn VARCHAR(200) 
                           , Hash VARCHAR(200) 
                           ); 

-- Create Audit table 
CREATE TABLE #Audit    ( 
                             KeyColumn     VARCHAR(200) 
                           , Message      VARCHAR(300) 
                           ); 

-- Since columns are manually entered, there might be extra spaces
SET @KeyColumns       = REPLACE(@KeyColumns, ' ', ''); 
SET @KeyColumns       = REPLACE(@KeyColumns, ',', ',''' + @Separator + ''','); 
SET @ExceptionColumns = REPLACE(@ExceptionColumns, ' ', '')

-- Prevent space in the delimiter column name
SET @LimitColumn      = QUOTENAME(@LimitColumn) 

-- Create cursor to read all column names from the production environment table
DECLARE cursor1 CURSOR FOR 
    SELECT c.name ,t.name AS typ 
    FROM sys.columns     c 
    JOIN sys.types       t ON t.user_type_id = c.user_type_id 
    JOIN sys.objects     o ON o.object_id = c.object_id 
    WHERE o.name = @ProductionTable 
      AND o.type = 'U' 
      AND ( 
               -- No exception column provided
               LEN(@ExceptionColumns) = 0 OR              
               -- Prevent excluding a column by partial name matching
               CHARINDEX(',' + c.name + ',', ',' +@ExceptionColumns + ',')= 0 
          ) 

OPEN cursor1; 


FETCH NEXT FROM cursor1 INTO @ColumnName ,@DataType;

WHILE @@FETCH_STATUS = 0 
BEGIN 

DECLARE @expr NVARCHAR(MAX); 
-- Handle column names that might contain spaces 

SET @ColumnName = QUOTENAME(@ColumnName) 

    -- Standardized expression by type for HASH/DETAIL 
    IF @DataType IN ('date','datetime','smalldatetime','datetime2','datetimeoffset','time') 
        SET @expr = N'ISNULL(CONVERT(CHAR(33), ' + @ColumnName + ', 126), '''')'; 
    ELSE IF @DataType IN ('uniqueidentifier') 
        SET @expr = N'ISNULL(CONVERT(CHAR(36), ' + @ColumnName +'), '''')'; 
    ELSE IF @DataType IN ('int','bigint','smallint','tinyint','bit','decimal','numeric','money','smallmoney','float','real')
        SET @expr = N'ISNULL(CONVERT(VARCHAR(100), '+ @ColumnName +'), '''')'; 
    ELSE 
        SET @expr = N'ISNULL(CAST('+ @ColumnName +' AS NVARCHAR(MAX)), '''')'; 


    IF @CheckCaseSensitivity = 0 
        SET @Columns += REPLICATE(' ', 57) + @expr + ',' + CHAR(13);
    ELSE 
        SET @Columns += REPLICATE(' ', 57) + N'UPPER(' + @expr + '),'  + CHAR(13);

    -- Load next cursor line 
    FETCH NEXT FROM cursor1 INTO @ColumnName ,@DataType; 
END 

-- Close and clean up cursor 
CLOSE cursor1; 
DEALLOCATE cursor1; 

-- Remove last comma 
SET @Columns             = LEFT(@Columns, LEN(@Columns) - 2) 


------------------------------------------------------------
-- Create Hash Tables
------------------------------------------------------------
-- Prevent space in table names 
SET @ProductionTable      = QUOTENAME(@ProductionTable) 
SET @StagingTable         = QUOTENAME(@StagingTable) 

-- Build SQL for production table 
SET @SQL = N'
INSERT INTO #ProdHash 
SELECT CONCAT(' + @KeyColumns + N') AS KeyColumn, 
CONVERT(VARCHAR(64), HASHBYTES(''SHA2_256'', CONCAT(' + CHAR(13) + @Columns + N')),2) AS Hash 
FROM ' + @ProductionTable + N';'; 


-- If the limit column was provided, update the SQL to consider it (Include WHERE Column...) 
IF LEN(@LimitColumn) > 0 
BEGIN 
    SET @SQL = LEFT(@SQL, LEN(@SQL) - 1); 
    SET @SQL += N' WHERE ' + @LimitColumn + N' = (SELECT MAX(' + @LimitColumn + N') FROM ' + @ProductionTable + N');'; 
END 


IF @DebugMode = 1 
BEGIN 
    PRINT '------------------------------------------------------------------------------'
    PRINT 'Production Table Query'
    PRINT '------------------------------------------------------------------------------'
    PRINT CHAR(13) + @SQL + CHAR(13)
END

-- Load production Hash table
EXEC (@SQL) 


-- Build SQL for staging table 
SET @SQL = N'INSERT INTO #StagingHash 
SELECT CONCAT(' + @KeyColumns + N') AS KeyColumn, 
CONVERT(VARCHAR(64), HASHBYTES(''SHA2_256'', CONCAT(' + CHAR(13) + @Columns + N')),2) AS Hash 
FROM ' + @StagingTable + ';'; 


IF @DebugMode = 1 
BEGIN 
    PRINT '------------------------------------------------------------------------------'
    PRINT 'Staging Table Query'
    PRINT '------------------------------------------------------------------------------'
    PRINT CHAR(13) + @SQL + CHAR(13)

END

-- Load staging Hash table
EXEC (@SQL) 

------------------------------------------------------------
-- Calculate Deltas
------------------------------------------------------------

PRINT '------------------------------------------------------------------------------'
PRINT 'Delta Calculation'
PRINT '------------------------------------------------------------------------------'


SELECT @ProdCount = COUNT_BIG(*)  FROM #ProdHash 
SELECT @StagingCount  = COUNT_BIG(*)  FROM #StagingHash 


-- Load into the audit table records that were excluded (exist in Prod, not in Staging)
INSERT INTO #Audit
SELECT KeyColumn , 'Record excluded'
FROM (
    SELECT KeyColumn 
    FROM #ProdHash 
    EXCEPT 
    SELECT KeyColumn 
    FROM #StagingHash 
 ) AS Delta;
 

-- Load into the audit table records that are new (exist in Staging, not in Prod)
INSERT INTO #Audit
SELECT KeyColumn, 'New record'
FROM (
    SELECT KeyColumn 
    FROM #StagingHash
    EXCEPT 
    SELECT KeyColumn 
    FROM #ProdHash 
) AS Delta;

-- Load into the audit table records that have an identical key but an unexpected difference 
INSERT INTO #Audit
SELECT  A.KeyColumn
      ,'Identical key, but with unexpected difference'
FROM #ProdHash A 
INNER JOIN #StagingHash B     ON B.KeyColumn = A.KeyColumn 
                              AND B.Hash <> A.Hash; 


------------------------------------------------------------
-- Final Delta Calculations
------------------------------------------------------------
-- Excluded Delta (quantity might be equal, but record is absent compensated by an inclusion) 
SELECT @DeltaExcludedCount    = COUNT_BIG(*)  FROM  #Audit WHERE Message = 'Record excluded'

-- Included Delta (quantity might be equal, but record is new compensated by an exclusion) 
SELECT @DeltaIncludedCount    = COUNT_BIG(*)  FROM  #Audit WHERE Message = 'New record'

-- Calculate count of records with identical key, but different Hash (Unexpected) 
SELECT @DeltaHashCount          = COUNT_BIG(*)  FROM  #Audit WHERE Message = 'Identical key, but with unexpected difference'


-- Check if quantities match 
IF @ProdCount <> @StagingCount 
    PRINT 'Quantity Divergence: Production with ' + FORMAT(@ProdCount, 'N0', 'pt-BR') + ' records ' + CHAR(13) + 
          '                     Staging with ' + FORMAT(@StagingCount, 'N0', 'pt-BR') + ' records ' 
ELSE 
    PRINT 'Identical record count in both tables: ' + FORMAT(@ProdCount, 'N0', 'pt-BR') + ' records ' 
    

-- Print the quantity of each Delta 
IF @DeltaExcludedCount > 0      
    PRINT 'Were excluded '    + FORMAT(@DeltaExcludedCount, 'N0', 'pt-BR') + ' records' 

IF @DeltaIncludedCount > 0      
    PRINT 'Were included '    + FORMAT(@DeltaIncludedCount, 'N0', 'pt-BR') + ' new records' 
                            
IF @DeltaHashCount > 0 
    PRINT 'Were found ' + FORMAT(@DeltaHashCount, 'N0', 'pt-BR') + ' records with identical key, but with unexpected difference' 


PRINT '------------------------------------------------------------------------------'


SELECT * FROM #Audit
