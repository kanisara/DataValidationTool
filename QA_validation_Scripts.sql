/**********  QA Validate Scripts  *********/
DROP TABLE dm_nz_migration_sbox.QA_Validation_Status
GO
CREATE TABLE dm_nz_migration_sbox.QA_Validation_Status ( 
RunID  INt Identity (1,1),
SourceSystem VARCHAR(20),
TargetSystem VARCHAR(20),
SourceSchema VARCHAR(50),
TargetSchema VARCHAR(50),
TableName VARCHAR(100),
SourceTblCnt int,
TargetTblCnt int,
SourceMinusTargetCnt  int,
TargetMinusSourceCnt  int,
TestedDate DateTime,
TestStatus varchar(10)
)
GO
/* Generate data to QA validation status table *****
Author : Saravanakumar G
Date   :02-08-2020
***************************************************************/
DROP PROC dm_nz_migration_sbox.pr_etl_validate_update  
GO
CREATE PROC dm_nz_migration_sbox.pr_etl_validate_update -- 'stg_cog_dev_cds_genius' ,'T_BATCH_LOAD_STATUS'
@SchemaName varchar(30),
@tablename varchar(100)
--,@SSchemaName varchar(30)
AS
BEGIN
SET NOCOUNT ON
DECLARE @tablid INT ,@SourceMinusTargetCnt int , @TargetMinusSourceCnt INT , @SourceTblCnt int, @TargetTblCnt int
DECLARE @SMT varchar(5000) , @TMS varchar(5000) , @SSchemaName varchar(30) 

SET @SSchemaName ='dm_nz_migration_sbox'

--IF  EXISTS( SELECT * FROM sys.objects where schema_id = schema_id (@SSchemaName) and name = '#SMT_sc')
--BEGIN
--    DROP table [#SMT_sc]
--END

--IF  EXISTS( SELECT * FROM sys.objects where schema_id = schema_id (@SSchemaName) and name = '#TMS_sc')
--BEGIN
--    DROP table [#TMS_sc]
--END

--IF  EXISTS( SELECT * FROM sys.objects where schema_id = schema_id (@SSchemaName) and name = '#SMT_m')
--BEGIN
--    DROP table [#SMT_m]
--END

--IF  EXISTS( SELECT * FROM sys.objects where schema_id = schema_id (@SSchemaName) and name = '#TMS_m')
--BEGIN
--    DROP table [#TMS_m]
--END
DROP TABLE [#SMT_sc];
DROP table [#TMS_sc];
DROP table [#SMT_m];
DROP table [#TMS_m]


CREATE table #SMT_sc (sc int ) 
CREATE table #TMS_sc (tc int ) 
CREATE table #SMT_m (smt int) 
CREATE table #TMS_m (tms int) 

/*   Generating List of selected columns */

Declare @MAXCNT INT , @i INT, @Col_Name VARCHAR(100), @Col_List varchar(max) , @listStrTQuery Varchar(max)
SET @MAXCNT = (SELECT Count(*) 
					FROM SYS.TABLES t    
					INNER JOIN SYS.COLUMNS c ON c.OBJECT_ID = t.OBJECT_ID 
					INNER JOIN sys.schemas s ON S.schema_id  = t.schema_id
					--WHERE S.NAME ='stg_cog_dev_cds_genius' AND t.name ='T_BATCH_LOAD_STATUS' )
					WHERE S.NAME =@SchemaName AND t.name =@TableName )

SET @i=1
WHILE   @i <= @MAXCNT 
BEGIN
    set @Col_Name = (SELECT c.name
					FROM SYS.TABLES t    
					INNER JOIN SYS.COLUMNS c ON c.OBJECT_ID = t.OBJECT_ID 
					INNER JOIN sys.schemas s ON S.schema_id  = t.schema_id
					--WHERE S.NAME ='stg_cog_dev_cds_genius' AND t.name ='T_BATCH_LOAD_STATUS' AND c.column_id =@i)
					WHERE S.NAME =@SchemaName AND t.name =@TableName AND c.column_id =@i )

	IF Len(@Col_List) > 0 
		SET @Col_List = @Col_List + ',' + @Col_Name
	else
		SET @Col_List = @Col_Name
	
	SET  @i +=1;
	
END

--SELECT @Col_List

/*   Ignoring Load_Time column on A-B operation */

SET @listStrTQuery = (select replace(@Col_List,',Load_Time',''))

--SET @listStrTQuery = (SELECT @Col_List)

/* Generating value for @SourceTblCnt and @TargetTblCnt  */

SET @SMT = 'insert into #SMT_sc select count(*) from ' + @SSchemaName + '.' + @tablename 
SET @TMS = 'insert into #TMS_sc select count(*) from ' + @SchemaName + '.' + @tablename 

exec (@SMT)
exec (@TMS)

SET @SourceTblCnt = (select * from #SMT_sc)
SET @TargetTblCnt = (select * from #TMS_sc)

/* Generating value for @SourceMinusTargetCnt and @TargetMinusSourceCnt  */

SET @SMT = 'insert into #SMT_m select count(*) from (select ' + @listStrTQuery + ' from ' + @SSchemaName + '.' + @tablename  + ' except SELECT ' + @listStrTQuery + ' FROM ' + @SchemaName + '.' + @tablename + ' ) H '
SET @TMS = 'insert into #TMS_m select count(*) from (select ' + @listStrTQuery + ' from ' + @SchemaName + '.' + @tablename  + ' except SELECT ' + @listStrTQuery + ' FROM ' + @SSchemaName + '.' + @tablename + ' ) H '

exec (@SMT)
exec (@TMS)

set @SourceMinusTargetCnt = (select * from #SMT_m)
set @TargetMinusSourceCnt = (select * from #TMS_m) 

/* Loading data into QA validation Table */

--SELECT @SSchemaName

	insert into [dm_nz_migration_sbox].[QA_Validation_Status]  ([SourceSystem],[TargetSystem],[SourceSchema],[TargetSchema],[TableName],[SourceTblCnt],[TargetTblCnt],[SourceMinusTargetCnt],[TargetMinusSourceCnt],[TestedDate],[TestStatus])
	SELECT 'Netezza','Azure-SQLDW',@SSchemaName,@SchemaName,@tablename,@SourceTblCnt,@TargetTblCnt,@SourceMinusTargetCnt,@TargetMinusSourceCnt,getdate(),CASE WHEN (@SourceMinusTargetCnt - @TargetMinusSourceCnt) =0 then 'PASS' else 'FAIL' end 

END 

GO
---Final Validation Report 
DROP PROC dm_nz_migration_sbox.PR_QA_VALIDATE_UPDATE
GO
CREATE proc dm_nz_migration_sbox.PR_QA_VALIDATE_UPDATE  --'dm_cog_dev_LIFEOPSDASH'
@SchemaName varchar(30)
AS
BEGIN
SET NOCOUNT ON
DECLARE @SQL VARCHAR(MAX)
--SET @SQL = ( SELECT  'EXECUTE dm_nz_migration_sbox.pr_etl_validate_update ''' + @SchemaName + ''',''' + NAME + ''';' FROM SYS.OBJECTS
--				WHERE TYPE ='U' AND SCHEMA_ID =SCHEMA_ID ('DM_NZ_MIGRATION_SBOX') 
--				AND NAME NOT IN ('TESTRESULTS' ,'QA_VALIDATION_STATUS' )   )

SELECT  'EXECUTE dm_nz_migration_sbox.pr_etl_validate_update ''' + @SchemaName + ''',''' + NAME + ''';' FROM SYS.OBJECTS
				WHERE TYPE ='U' AND SCHEMA_ID =SCHEMA_ID ('DM_NZ_MIGRATION_SBOX') 
				AND NAME NOT IN ('TESTRESULTS' ,'QA_VALIDATION_STATUS' ) 

END



-- select * from dm_nz_migration_sbox.QA_Validation_Status

/* 
select SOURCE_SYSTEM_ID,SOURCE_SYSTEM_INSTANCE_ID,BATCH_ID,STATUS,BATCH_START_DATE,BATCH_END_DATE,SPARE_1,SPARE_2,SPARE_3,SPARE_4 from stg_cog_dev_CDS_GENIUS.T_BATCH_CONTROL 
except
select SOURCE_SYSTEM_ID,SOURCE_SYSTEM_INSTANCE_ID,BATCH_ID,STATUS,BATCH_START_DATE,BATCH_END_DATE,SPARE_1,SPARE_2,SPARE_3,SPARE_4 from dm_nz_migration_sbox.T_BATCH_CONTROL 


 */