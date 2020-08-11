/*  Generate ETL validation Tables on Cloud Azure Dataabse *****
Author : Saravanakumar G
Date   :10-06-2020
***************************************************************/
DROP TABLE ETLMaster 
GO
CREATE TABLE ETLMaster(  
TableID INt identity ,
SourceSystem  Varchar (50),
SourceDB  varchar(30),
SourceTable  varchar(100),
TargetSystem  Varchar (50),
TargetDB  varchar(30),
TargetTable  varchar(50),
Active  int,
TPrimaryKey  varchar(10),
CDCCloumnName  varchar(10),
LastUpdateFrom    varchar(20),
LastUpdateTo    varchar(20)
)
GO
DROP TABLE Transaction_Master
GO
CREATE TABLE Transaction_Master ( 
RunID  INt Identity ,
JobID INT,
TableID INT,
SourceTblCnt int,
TargetTblCnt int,
SourceTblLoadDuration int,
TargetTblLoadDuration Int,
SourceMinusTargetCnt  int,
TargetMinusSourceCnt  int,
LoadType  Varchar(10),
RunDate varchar(20),
TestStatus varchar(10)
)
GO
DROP TABLE Job_Master
GO
CREATE TABLE Job_Master ( 
JobID INT,
JobStartTime Datetime,
JobEndTime Datetime
)
GO
/* Create Store Procedure for batch ID Generation */
DROP PROC pr_jobid
GO
CREATE PROC pr_jobid
AS
BEGIN
SELECT (ISNULL(MAX(JobID),0) + 1 ) AS "JOBID" FROM Job_Master
END
GO
/*  Update Job Starttime on JOb Master table e */
DROP  proc pr_job_updateStartTime
GO
CREATE proc pr_job_updateStartTime
@JobID INT,
@JobStartTime datetime
as
begin
set nocount on
     insert into Job_Master (JobID,JobStartTime) values (@jobID,getdate())
end 
GO
/*  Update Job EndTime on JOb Master table  */
DROP  proc pr_job_updateEndTime
GO
CREATE proc pr_job_updateEndTime
@JobID INT,
@JobEndTime datetime
as
begin
set nocount on
     Update Job_Master set JobEndTime =getdate() where JobId =@jobID
end 
GO
/*  Update ETL Master table from Store Procedure */
DROP proc pr_etl_validate_etlmaster_update
GO
Create proc pr_etl_validate_etlmaster_update
@SourceSystem Varchar(10),
@SourceDB Varchar(10),
@Sourcetablename varchar(30),
@targerSystem Varchar(10),
@targetDB Varchar(10),
@targettablename varchar(30)
as
begin
set nocount on
DECLARE @tablid INT ,@SourceMinusTargetCnt int , @TargetMinusSourceCnt INT
declare @LUT varchar(20) , @LUF varchar(20)
declare @flg int
set @flg = (select count(*) from ETLMaster where SourceTable =@Sourcetablename)
if @flg = 1
		    IF  EXISTS (select LastUpdateTo from ETLMaster where SourceTable=@Sourcetablename and LastUpdateTo is null)
				update ETLMaster set LastUpdateTo =CONVERT(varchar, getdate(), 120) where SourceTable =@Sourcetablename
			else
				update ETLMaster set LastUpdateFrom =LastUpdateTo where SourceTable =@Sourcetablename
				update ETLMaster set LastUpdateTo =CONVERT(varchar, getdate(), 120) where SourceTable =@Sourcetablename 
if @flg = 0
		  insert into ETLMaster (SourceSystem,SourceDB,SourceTable,TargetSystem,TargetDB,TargetTable,Active,LastUpdateFrom) 
		  values (@SourceSystem,@SourceDB,@Sourcetablename,@targerSystem,@targetDB,@targettablename,1,CONVERT(varchar, getdate(), 120))
end 

/*  Update ETL Transaction Master table from Store Procedure */
GO
DROP proc pr_etl_validate_update   --1,'ChubbOnCloud','regions',10,4,4,10,'FULL'
GO
CREATE proc pr_etl_validate_update
@JobID INT,
@targetdbname varchar(50),
@tablename varchar(30),
@SourceTblLoadDuration int,
@SourceTblCnt int,
@TargetTblCnt int,
@TargetTblLoadDuration int,
@LoadType varchar(10)
as
begin
set nocount on
declare @SMT_a TABLE (smt int) 
declare @TMS_a TABLE (tms int) 
DECLARE @tablid INT ,@SourceMinusTargetCnt int , @TargetMinusSourceCnt INT
DECLARE @SMT varchar(5000) , @TMS varchar(5000)
DECLARE @listStrT VARCHAR(MAX)
SELECT @listStrT = COALESCE(@listStrT+',' ,'') + Name
FROM sys.columns where object_id=object_id('S_' + @tablename)

set @SMT = 'select count(*) from (select ' + @listStrT + ' from ' + @targetdbname + '..S_' + @tablename  + ' except SELECT ' + @listStrT + ' FROM ' + @targetdbname + '..T_' + @tablename + ' ) H '
set @TMS = 'select count(*) from (select ' + @listStrT + ' from ' + @targetdbname + '..T_' + @tablename  + ' except SELECT ' + @listStrT + ' FROM ' + @targetdbname + '..S_' + @tablename + ' ) H '

insert into @SMT_a  exec (@SMT)
insert into @TMS_a  exec (@TMS)

set @SourceMinusTargetCnt = (select * from @SMT_a)
set @TargetMinusSourceCnt = (select * from @TMS_a) 
SET @tablid = (SELECT TableID from ETLMaster where SourceTable=@tablename )

	insert into Transaction_Master (JobID,TableID,SourceTblCnt,TargetTblCnt,SourceTblLoadDuration,TargetTblLoadDuration,SourceMinusTargetCnt,TargetMinusSourceCnt,LoadType,RunDate,TestStatus)
	values (@JobID,@tablid,@SourceTblCnt,@TargetTblCnt,@SourceTblLoadDuration,@TargetTblLoadDuration,@SourceMinusTargetCnt,@TargetMinusSourceCnt,@LoadType,CONVERT(varchar, getdate(), 120), CASE WHEN (@SourceMinusTargetCnt - @TargetMinusSourceCnt) =0 then 'PASS' else 'FAIL' end  )
END 
GO
/* Update LastloadTime for AzureSQL Instance source & Traget tables */
drop proc pr_tardb_tbl_last_load_upd 
go
create proc pr_tardb_tbl_last_load_upd --'regions' , 'full'
@tablename varchar(100),
@LoadType varchar(10)
as
begin
declare @sql1 varchar(100),@sql2 varchar(100) ,@Lupd varchar(20), @s_tbl varchar(100) , @t_tbl varchar(100)
set @Lupd=(SELECT CONVERT(varchar, getdate(), 120))
set @s_tbl = 'S_'+@tablename
set @t_tbl = 'T_'+@tablename

IF NOT EXISTS( SELECT * FROM sys.columns  WHERE Name = 'LastUpdate'
      AND Object_ID = Object_ID(@s_tbl))
BEGIN
    set @sql1='ALTER TABLE S_' +@tablename + ' ADD LastUpdate varchar(25)'
	exec (@sql1)
END

IF NOT EXISTS( SELECT * FROM sys.columns  WHERE Name = 'LastUpdate'
      AND Object_ID = Object_ID(@t_tbl))
BEGIN
    set @sql1='ALTER TABLE T_' +@tablename + ' ADD LastUpdate varchar(25)'
	exec (@sql1)
END

if @LoadType ='FULL'
begin
	set @sql1 = 'update S_' +@tablename + ' set LastUpdate = ''' + @Lupd + ''''
	set @sql2 = 'update T_' +@tablename + ' set LastUpdate = ''' + @Lupd + ''''
end

if @LoadType ='DELTA'
begin
	set @sql1 = 'update S_' +@tablename + ' set LastUpdate = ''' + @Lupd + ''' where LastUpdate IS NULL'
	set @sql2 = 'update T_' +@tablename + ' set LastUpdate = ''' + @Lupd + ''' where LastUpdate IS NULL'
end

exec (@sql1)
exec (@sql2)
end 
GO
/* To Get table last load time from ETL Master table */
/* To Get table last load time from ETL Master table */
drop proc pr_get_etl_master_tbl_LastUpdateFrom 
go
create proc pr_get_etl_master_tbl_LastUpdateFrom
@tablename varchar(100)
as
begin
declare @sql varchar(100) ,@Lupd Datetime
select  CASE WHEN LastUpdateTo IS NULL then CONVERT(varchar, LastUpdateFrom, 120) else CONVERT(varchar, LastUpdateTo, 120) end as LastUpdateFrom  from ETLMASTER where Sourcetable= @tablename
end 

