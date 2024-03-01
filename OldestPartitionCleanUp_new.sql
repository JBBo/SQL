CREATE PROCEDURE [dbo].[OldestPartitionCleanUp_new]
AS
BEGIN

	print ('start time is '+convert(varchar(30),getdate(),114))
	declare @part_num smallint
	declare @start_time datetime
	declare @end_time datetime
	declare @sqltext nvarchar(max)

	set @part_num = (select min(partition_number) from sys.partitions where partition_number>1)
	DECLARE @min_boudary_id smallint
	SET @min_boudary_id = (SELECT MIN(boundary_id) FROM sys.partition_range_values)
	SET @start_time = CAST((SELECT RV.[value]
	FROM sys.partition_range_values AS RV
	JOIN sys.partition_functions AS PF
		ON RV.function_id = PF.function_id
	WHERE PF.[name] = 'OCS_PART_FUNC' AND RV.boundary_id=@min_boudary_id) AS datetime)

	SET @end_time = CAST((SELECT RV.[value]
	FROM sys.partition_range_values AS RV
	JOIN sys.partition_functions AS PF
		ON RV.function_id = PF.function_id
	WHERE PF.[name] = 'OCS_PART_FUNC' AND RV.boundary_id=@min_boudary_id+1) AS datetime)

	declare @min_fg varchar(8)
	declare @part_name varchar(6)

	set @min_fg=(SELECT DISTINCT fg.name
	FROM  sys.allocation_units au
		INNER JOIN sys.partitions p
		ON au.container_id = p.hobt_id
		INNER JOIN sys.filegroups fg
		ON fg.data_space_id = au.data_space_id
	WHERE p.partition_number=@part_num)
	SET @part_name = SUBSTRING(@min_fg,3,6)

/*print @part_num
print @end_time
print @part_name
print @min_fg*/

	BEGIN TRY
	set @sqltext='
	create database OCS_'+@part_name+'_Staging
	ON
	( NAME = OCS_'+@part_name+'_Staging_dat,
		FILENAME = ''C:\Databases\OCS_'+@part_name+'_Staging_dat'+'.mdf'''+',
		SIZE = 30208MB,
		FILEGROWTH = 512MB)
	LOG ON
	( NAME = OCS_'+@part_name+'_Staging_log,
		FILENAME = ''C:\Databases\OCS_'+@part_name+'_Staging_log'+'.ldf'''+',
		SIZE = 64MB,
		FILEGROWTH = 32MB)'
	execute (@sqltext)
	print ('the staging database creation finished at '+convert(varchar(30),getdate(),114))

	BEGIN TRANSACTION
	set @sqltext='USE OCS
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_CBA_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[BALANCENAME] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CHARGEDVALUE] [bigint] NULL,
		[NEWBALANCE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON'
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+' WITH
	(
	DATA_COMPRESSION = PAGE
	)
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CBA_auto] ADD CONSTRAINT [staging_PRIMA_PIKE_CBA_auto_PK_PRIMA_PIKE_CBA] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[SEQ_NO] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext = @sqltext + ' [' + @min_fg + '] '
	set @sqltext = @sqltext+ ' 
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_CBA_auto_PRIMA_CBA_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_CBA_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext = @sqltext + '[' + @min_fg + '] '

	set @sqltext = @sqltext + '
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CBA_auto] WITH CHECK ADD CONSTRAINT [chk_staging_PRIMA_PIKE_CBA_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CBA_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_CBA_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_CBA] SWITCH PARTITION '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_CBA_auto]'
	set @sqltext=@sqltext+'
	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_CBA_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_CBA_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_CBA_auto]'

	---ADM---19.07.2016
	set @sqltext=@sqltext+' 
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_ADM_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[ACCESSKEY] [varchar](32) NULL,
		[OWNINGCUSTOMERID] [varchar](1000) NULL,
		[EVENTTYPE] [int] NULL,
		[EFFECTIVEEVENTTIME] [datetime] NULL,
		[BILLCYCLEID] [bigint] NULL,
		[ERRORCODE] [bigint] NULL,
		[RATEEVENTTYPE] [varchar](1000) NULL,
		[EXTERNALCORRELATIONID] [varchar](1000) NULL,
		[CUSTOMERROPSCRMTITLE] [varchar](100) NULL,
		[ADMINRATINGPARAMADMINEVENT] [varchar](1000) NULL,
		[CUSTOMERROPSOFFERID] [varchar](1000) NULL,
		[ONTOUCHHANDLINGLOGINFO] [varchar](1000) NULL,
		[EVENTFARMNODEID] [varchar](1000) NULL,
		[EVENTPROCESSID] [varchar](1000) NULL,
		[EVENTCONTEXTID] [varchar](1000) NULL,
		[ENTRYSS7DIALEDNUMBER] [varchar](1000) NULL,
		[SS7NORMALIZEDCALLED] [varchar](1000) NULL,
		[SS7NORMALIZEDCALLER] [varchar](1000) NULL,
		[EVENTDURATION] [bigint] NULL,
		[EVENTCALLSETUPTIME] [bigint] NULL,
		[EVENTVOLUME] [bigint] NULL,
		[EVENTSTOPTIME] [bigint] NULL,
		[EVENTREGUIDINGINFO] [bigint] NULL,
		[EVENTISSECONDARY] [varchar](100) NULL,
		[WMCAEVENTORIGIN] [bigint] NULL,
		[EVENTREGUIDINGTARGETOFFERID] [bigint] NULL,
		[EVENTREGUIDINGFALLBACKOFFERID] [bigint] NULL,
		[EVENTORIGINALEVENTID] [bigint] NULL,
		[CUSTOMERUSERIMSI] [varchar](1000) NULL,
		[CALLREFERENCENUMBER] [varchar](1000) NULL,
		[EVENTINFO] [varchar](1000) NULL,
		[EXTERNALCLIENTUSERNAME] [varchar](1000) NULL,
		[EXTERNALCLIENTIPADDRESS] [varchar](1000) NULL,
		[EXTERNALCLIENTIFTYPE] [varchar](1000) NULL,
		[CABALANCEADJUSTMENTEVENTINFO] [varchar](1000) NULL,
		[CABALANCEADJUSTMENTCHARGECODE] [varchar](1000) NULL,
		[CABALANCEADJUSTMENTBALANCEID] [varchar](1000) NULL,
		[BALANCEADJUSTMENTAMOUNTOFUNITS] [varchar](1000) NULL,
		[BALANCEADJUSTMENTTRANSACTIONID] [varchar](1000) NULL,
		[ADMINUSECASE] [bigint] NULL,
		[ADMINKIND] [bigint] NULL,
		[CAMODIFYPACKAGEITEMCHARGEMODE] [bigint] NULL,
		[FYPACKAGEITEMBALANCEADJUSTMODE] [bigint] NULL,
		[YPACKAGEITEMBALANCEADJUSTVALUE] [bigint] NULL,
		[AGEITEMBALANCEADJUSTCHARGECODE] [bigint] NULL,
		[CKAGEITEMFOCSUBSCRIBEAVAILABLE] [bigint] NULL,
		[YPACKAGEITEMFOCMODIFYAVAILABLE] [bigint] NULL,
		[MODIFYPACKAGEITEMENDOFVALIDITY] [datetime] NULL,
		[MODIFYPACKAGEITEMCUGLISTAPPEND] [varchar](4000) NULL,
		[MODIFYPACKAGEITEMCUGLISTREMOVE] [varchar](4000) NULL,
		[MODIFYPACKAGEITEMCUGLISTDELETE] [varchar](4000) NULL,
		[CKAGEITEMFREEOFCHARGEAVAILABLE] [bigint] NULL,
		[KAGEITEMFAVORITEAREALISTAPPEND] [varchar](4000) NULL,
		[KAGEITEMFAVORITEAREALISTREMOVE] [varchar](4000) NULL,
		[KAGEITEMFAVORITEAREALISTDELETE] [varchar](4000) NULL,
		[FYPACKAGEITEMACCOUNTEXPIRYDATE] [datetime] NULL,
		[AGEITEMACCOUNTEXPIRYDATEPOLICY] [bigint] NULL,
		[YPACKAGEITEMSECONDSOFEXTENSION] [bigint] NULL,
		[MODIFYPACKAGEITEMFNFLISTAPPEND] [varchar](4000) NULL,
		[MODIFYPACKAGEITEMFNFLISTREMOVE] [varchar](4000) NULL,
		[MODIFYPACKAGEITEMFNFLISTDELETE] [varchar](4000) NULL,
		[MODIFYPACKAGEITEMFNFLISTUSEPID] [varchar](4000) NULL,
		[YPACKAGEITEMHOMEZONELISTAPPEND] [varchar](4000) NULL,
		[YPACKAGEITEMHOMEZONELISTREMOVE] [varchar](4000) NULL,
		[YPACKAGEITEMHOMEZONELISTDELETE] [varchar](4000) NULL,
		[CAMODIFYPACKAGEITEMCREDITLIMIT] [bigint] NULL,
		[MODIFYPACKAGEITEMLIFETIMESTART] [datetime] NULL,
		[CAREADPACKAGEITEMCHARGEMODE] [bigint] NULL,
		[CAREADPACKAGEITEMPRICES] [varchar](100) NULL,
		[SUBSCRIBEPACKAGEITEMCHARGEMODE] [bigint] NULL,
		[CASUBSCRIBEPACKAGEITEMRECHARGE] [bigint] NULL,
		[SUBSCRIBEPACKAGEITEMCHARGECODE] [bigint] NULL,
		[CRIBEPACKAGEITEMACTIVATIONTIME] [datetime] NULL,
		[RIBEPACKAGEITEMUNSUBSCRIBETIME] [datetime] NULL,
		[SCRIBEPACKAGEITEMLIFETIMESTART] [datetime] NULL,
		[TMAFREQUESTPACKAGE] [varchar](1000) NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[TOFFSET] [numeric](4, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+' WITH
	(
	DATA_COMPRESSION = PAGE
	)
	
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_ADM_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_ADM_auto_PK_PRIMA_PIKE_ADM] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext = @sqltext + ' [' + @min_fg + '] '
	set @sqltext = @sqltext+ ' 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_ADM_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_ADM_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_ADM_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_ADM_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_ADM] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_ADM_auto]'
	set @sqltext=@sqltext+'
	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_ADM_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_ADM_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_ADM_auto]'

	---CCP---
	set @sqltext=@sqltext+' 
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_CCP_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[CHARGECODE] [bigint] NULL,
		[CHARGEBEGBIGINTIME] [bigint] NULL,
		[CHARGEDCALLDURATION] [bigint] NULL,
		[USEDCALLDURATION] [bigint] NULL,
		[CHARGEDVOLUME] [bigint] NULL,
		[USEDVOLUME] [bigint] NULL,
		[CHARGETYPE] [bigint] NULL,
		[CHARGEDPRICE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+' WITH
	(
	DATA_COMPRESSION = PAGE
	)

	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CCP_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_CCP_auto_PK_PRIMA_PIKE_CCP] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[SEQ_NO] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext = @sqltext + ' [' + @min_fg + '] '
	set @sqltext = @sqltext+ '
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_CCP_auto_PRIMA_PIKE_CCP_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_CCP_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext = @sqltext + ' [' + @min_fg + '] '
	set @sqltext = @sqltext+ ' 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CCP_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_CCP_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CCP_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_CCP_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_CCP] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_CCP_auto]'
	set @sqltext=@sqltext+'
	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_CCP_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_CCP_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_CCP_auto]'

	---CONF---
	set @sqltext=@sqltext+'
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_CONF_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[ACCESSKEY] [varchar](32) COLLATE Cyrillic_General_CI_AS NULL,
		[OWNINGCUSTOMERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EVENTTYPE] [int] NULL,
		[EFFECTIVEEVENTTIME] [datetime] NULL,
		[BILLCYCLEID] [bigint] NULL,
		[ERRORCODE] [bigint] NULL,
		[RATEEVENTTYPE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EXTERNALCORRELATIONID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSCRMTITLE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSOFFERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEOFFERSUBSCRIPTIONTARGETOFFER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[ARGINGPARAMETERSRECHARGEAMOUNT] [bigint] NULL,
		[AMETERSEXPIRYDATEEXTENSIONMODE] [bigint] NULL,
		[GPARAMETERSEXPIRYDATEEXTENSION] [bigint] NULL,
		[GINGPARAMETERSRECHARGECURRENCY] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[ECHARGINGPARAMETERSVOUCHERTYPE] [bigint] NULL,
		[INGPARAMETERSVOUCHEREXPIRYDATE] [bigint] NULL,
		[RGINGPARAMETERSVOUCHERSERIALNO] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RECHARGINGPARAMETERSVOUCHERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RECHARGINGPARAMETERSTRANSID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[URECHARGINGINRECHARGECHANNEL] [bigint] NULL,
		[URECHARGINGINEVENTNAME] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,	
		[HTZ] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[BALANCENAME] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[INCREMENTVALUE] [bigint] NULL,
		[NEWBALANCE] [bigint] NULL,
		[RESULTINGEXPIRATIONDATE] [datetime] NULL,
		[RESULTINGMTCEXPIRATIONDATE] [datetime] NULL,
		[EC] [bigint] NULL,
		[OLDOFFER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[NEWOFFER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[TOFFSET] [numeric](4, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	WITH
	(
	DATA_COMPRESSION = PAGE
	)
 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CONF_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_CONF_auto_PK_PRIMA_PIKE_CONF] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON'
	set @sqltext = @sqltext + ' [' + @min_fg + '] '
	set @sqltext = @sqltext+ '
 
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_CONF_auto_PRIMA_PIKE_CONF_ACC_IDX] ON [dbo].[staging_PRIMA_PIKE_CONF_auto]
	(
		[ACCESSKEY] ASC,
		[EVENTTYPE] ASC
	)
	INCLUDE ( 	[PARTTIME]) WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext = @sqltext + ' [' + @min_fg + '] '
	set @sqltext = @sqltext+ '
 
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_CONF_auto_PRIMA_PIKE_CONF_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_CONF_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext = @sqltext + ' [' + @min_fg + '] '

	--next added on 30.06.2014
	set @sqltext = @sqltext+ '
	CREATE NONCLUSTERED INDEX [staging_PIKE_PIKE_CONF_auto_prima_pike_conf_perf] ON [dbo].[staging_PRIMA_PIKE_CONF_auto] 
	(
		[ACCESSKEY] ASC,
		[EFFECTIVEEVENTTIME] ASC
	)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON'
	set @sqltext = @sqltext + ' [' + @min_fg + '] '


	set @sqltext = @sqltext+ '
 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CONF_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_CONF_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CONF_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_CONF_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_CONF] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_CONF_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_CONF_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_CONF_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_CONF_auto]'

	---CPP---
	set @sqltext=@sqltext+' 
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_CPP_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[CHARGECODE] [bigint] NULL,
		[CHARGETYPE] [bigint] NULL,
		[CHARGEDPRICEUNITTYPE] [bigint] NULL,
		[CHARGEDPRICE] [bigint] NULL,
		[CHARGEDSPLITPRICES] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[TAXINFO] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	WITH
	(
	DATA_COMPRESSION = PAGE
	)

 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CPP_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_CPP_auto_PK_PRIMA_PIKE_CPP] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[SEQ_NO] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_CPP_auto_PRIMA_PIKE_CPP_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_CPP_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CPP_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_CPP_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_CPP_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_CPP_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_CPP] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_CPP_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_CPP_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_CPP_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_CPP_auto]'

	---GPRS
	set @sqltext=@sqltext+'  
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
		CREATE TABLE [dbo].[staging_PRIMA_PIKE_GPRS_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[ACCESSKEY] [varchar](32) COLLATE Cyrillic_General_CI_AS NULL,
		[OWNINGCUSTOMERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EVENTTYPE] [int] NULL,
		[EFFECTIVEEVENTTIME] [datetime] NULL,
		[BILLCYCLEID] [bigint] NULL,
		[ERRORCODE] [bigint] NULL,
		[RATEEVENTTYPE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EXTERNALCORRELATIONID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSCRMTITLE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[COMMONPARAMETERAPN] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGORIGINATORLOCATIONINFO] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDCALLED] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDCALLER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSOFFERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERUSERIMSI] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CALLREFERENCENUMBER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[FBCSVCQUOTATYPE] [bigint] NULL,
		[FBCROAMINGZONE] [bigint] NULL,
		[HTZ] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CHARGERESULT] [bigint] NULL,
		[REASONINFO] [bigint] NULL,
		[CALLSTOPTIME] [bigint] NULL,
		[CALLDURATION] [bigint] NULL,
		[DMITICKETSCORRELATIONID] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[DMITICKETSTERMINATECAUSE] [bigint] NULL,
		[DMITICKETSTICKETSEQUENCE] [bigint] NULL,
		[DMTRSSTICKETCATEGORY] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[ERRDMITICKETSEVENTCAUSE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[TOFFSET] [numeric](4, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	WITH
	(
	DATA_COMPRESSION = PAGE
	)

 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_GPRS_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_GPRS_auto_PK_PRIMA_PIKE_GPRS] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
		
	 
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_GPRS_auto_PRIMA_PIKE_GPRS_ACC_IDX] ON [dbo].[staging_PRIMA_PIKE_GPRS_auto]
	(
		[ACCESSKEY] ASC,
		[EVENTTYPE] ASC
	)
	INCLUDE ( [PARTTIME]) WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '

	--Added on 03.06.2014:
	set @sqltext=@sqltext+'

	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_GPRS_auto_prima_pike_gprs_perf] ON [dbo].[staging_PRIMA_PIKE_GPRS_auto]
	(
		[ACCESSKEY] ASC, 
		[EFFECTIVEEVENTTIME] ASC
	)
	WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	--

	set @sqltext=@sqltext+'

	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_GPRS_auto_PRIMA_PIKE_GPRS_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_GPRS_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '

	set @sqltext=@sqltext+'
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_GPRS_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_GPRS_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_GPRS_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_GPRS_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_GPRS] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_GPRS_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_GPRS_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_GPRS_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_GPRS_auto]'

	---IBA---
	set @sqltext=@sqltext+'  
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_IBA_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[BALANCENAME] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[INCREMENTVALUE] [bigint] NULL,
		[NEWBALANCE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	
	WITH
	(
	DATA_COMPRESSION = PAGE
	)
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_IBA_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_IBA_auto_PK_PRIMA_PIKE_IBA] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[SEQ_NO] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_IBA_auto_PRIMA_PIKE_IBA_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_IBA_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_IBA_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_IBA_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_IBA_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_IBA_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_IBA] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_IBA_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_IBA_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_IBA_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_IBA_auto]'

	---MMS---
	set @sqltext=@sqltext+'  
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_MMS_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[ACCESSKEY] [varchar](32) COLLATE Cyrillic_General_CI_AS NULL,
		[OWNINGCUSTOMERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EVENTTYPE] [int] NULL,
		[EFFECTIVEEVENTTIME] [datetime] NULL,
		[BILLCYCLEID] [bigint] NULL,
		[ERRORCODE] [bigint] NULL,
		[RATEEVENTTYPE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EXTERNALCORRELATIONID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSCRMTITLE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[ATIONMMSINFORMATIONMESSAGETYPE] [bigint] NULL,
		[ONORIGINATORADDRESSADDRESSDATA] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[MSINFORMATIONRECIPIENTADDRESSL] [bigint] NULL,
		[RATINGORIGINATORLOCATIONINFO] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGRECIPIENTL] [bigint] NULL,
		[RATINGMESSAGESIZE] [bigint] NULL,
		[RATINGVASID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGVASPID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGXNORMALIZEDDESTINATIONS] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGXTYPEOFDESTINATIONS] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[EVENTSTOPTIME] [bigint] NULL,
		[CUSTOMERROPSOFFERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERUSERIMSI] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CALLREFERENCENUMBER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXMERCHANTID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXPRODUCTID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXPURPOSE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[HTZ] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CHARGERESULT] [bigint] NULL,
		[REASONINFO] [bigint] NULL,
		[CALLSTOPTIME] [bigint] NULL,
		[CALLDURATION] [bigint] NULL,
		[DMIREQUESTSESSIONID] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[ONSUBMISSIONTIMEINMILLISECONDS] [datetime] NULL,
		[NSNPPIINFORMATIONNSNMETHODNAME] [bigint] NULL,
		[INFORMATIONNSNACCESSFRONTENDID] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[DMITICKETSEVENTCAUSE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[TOFFSET] [numeric](4, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	
	WITH
	(
	DATA_COMPRESSION = PAGE
	)
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_MMS_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_MMS_auto_PK_PRIMA_PIKE_MMS] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_MMS_auto_PRIMA_PIKE_MMS_ACC_IDX] ON [dbo].[staging_PRIMA_PIKE_MMS_auto]
	(
		[ACCESSKEY] ASC,
		[EVENTTYPE] ASC
	)
	INCLUDE ( 	[PARTTIME]) WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_MMS_auto_PRIMA_PIKE_MMS_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_MMS_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_MMS_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_MMS_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_MMS_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_MMS_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_MMS] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_MMS_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_MMS_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_MMS_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_MMS_auto]'

	---MOC 
	set @sqltext=@sqltext+'  
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_MOC_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[ACCESSKEY] [varchar](32) COLLATE Cyrillic_General_CI_AS NULL,
		[OWNINGCUSTOMERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EVENTTYPE] [int] NULL,
		[EFFECTIVEEVENTTIME] [datetime] NULL,
		[BILLCYCLEID] [bigint] NULL,
		[ERRORCODE] [bigint] NULL,
		[RATEEVENTTYPE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EXTERNALCORRELATIONID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSCRMTITLE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[DMTRASIAVPDATA] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDCALLED] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSOFFERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERUSERIMSI] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CALLREFERENCENUMBER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7ISROAMING] [varchar](100) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7ISCF] [varchar](100) COLLATE Cyrillic_General_CI_AS NULL,
		[RMALIZEDCALLEDWITHOUTMNPPREFIX] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDVLRNUMBER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7EXPANDEDCALLKIND] [bigint] NULL,
		[SS7IMSI] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGORIGINATORLOCATIONINFO] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[HTZ] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CHARGERESULT] [bigint] NULL,
		[REASONINFO] [bigint] NULL,
		[CALLSTOPTIME] [bigint] NULL,
		[CALLDURATION] [bigint] NULL,
		[GEXSESSIONID] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXREQUESTEDPARTYADDRESS] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXISFIRSTCALL] [bigint] NULL,
		[UISFIRSTANNOID] [bigint] NULL,
		[UISLASTANNOID] [bigint] NULL,
		[ERR1DMITICKETSEVENTCAUSE] [bigint] NULL,
		[ERR2DMITICKETSEVENTCAUSE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[TOFFSET] [numeric](4, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	
	WITH
	(
	DATA_COMPRESSION = PAGE
	)
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_MOC_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_MOC_auto_PK_PRIMA_PIKE_MOC] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_MOC_auto_PRIMA_PIKE_MOC_ACC_IDX] ON [dbo].[staging_PRIMA_PIKE_MOC_auto]
	(
		[ACCESSKEY] ASC,
		[EVENTTYPE] ASC
	)
	INCLUDE ( 	[PARTTIME]) WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_MOC_auto_PRIMA_PIKE_MOC_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_MOC_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_MOC_auto_PRIMA_PIKE_MOC_SEQ_NONCL_IDX] ON [dbo].[staging_PRIMA_PIKE_MOC_auto]
	(
		[SEQ_NO] ASC
	)
	INCLUDE ( 	[PARTTIME]) WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '

	--Added on 30.06.2014:
	set @sqltext=@sqltext+'

	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_MOC_auto_prima_pike_moc_perf] ON [dbo].[staging_PRIMA_PIKE_MOC_auto]
	(
		[ACCESSKEY] ASC, 
		[EFFECTIVEEVENTTIME] ASC
	)
	WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	--

	set @sqltext=@sqltext+'
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_MOC_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_MOC_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_MOC_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_MOC_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_MOC] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_MOC_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_MOC_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_MOC_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_MOC_auto]'

	---MTC
	set @sqltext=@sqltext+' 
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_MTC_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[ACCESSKEY] [varchar](32) COLLATE Cyrillic_General_CI_AS NULL,
		[OWNINGCUSTOMERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EVENTTYPE] [int] NULL,
		[EFFECTIVEEVENTTIME] [datetime] NULL,
		[BILLCYCLEID] [bigint] NULL,
		[ERRORCODE] [bigint] NULL,
		[RATEEVENTTYPE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EXTERNALCORRELATIONID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSCRMTITLE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[DMTRASIAVPDATA] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDCALLED] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSOFFERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERUSERIMSI] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CALLREFERENCENUMBER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7ISROAMING] [varchar](100) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7ISCF] [varchar](100) COLLATE Cyrillic_General_CI_AS NULL,
		[RMALIZEDCALLERWITHOUTMNPPREFIX] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDVLRNUMBER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7EXPANDEDCALLKIND] [bigint] NULL,
		[SS7IMSI] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGORIGINATORLOCATIONINFO] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[HTZ] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CHARGERESULT] [bigint] NULL,
		[REASONINFO] [bigint] NULL,
		[CALLSTOPTIME] [bigint] NULL,
		[CALLDURATION] [bigint] NULL,
		[GEXSESSIONID] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXREQUESTEDPARTYADDRESS] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXISFIRSTCALL] [bigint] NULL,
		[UISFIRSTANNOID] [bigint] NULL,
		[UISLASTANNOID] [bigint] NULL,
		[ERR1DMITICKETSEVENTCAUSE] [bigint] NULL,
		[ERR2DMITICKETSEVENTCAUSE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[TOFFSET] [numeric](4, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	
	WITH
	(
	DATA_COMPRESSION = PAGE
	)
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_MTC_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_MTC_auto_PK_PRIMA_PIKE_MTC] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_MTC_auto_PRIMA_PIKE_MTC_ACC_IDX] ON [dbo].[staging_PRIMA_PIKE_MTC_auto]
	(
		[ACCESSKEY] ASC,
		[EVENTTYPE] ASC
	)
	INCLUDE ( 	[PARTTIME]) WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '

	--Added on 30.06.2014:
	set @sqltext=@sqltext+'
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_MTC_auto_prima_pike_mtc_perf] ON [dbo].[staging_PRIMA_PIKE_MTC_auto] 
	(
		[ACCESSKEY] ASC,
		[EFFECTIVEEVENTTIME] ASC
	)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	--

	set @sqltext=@sqltext+'
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_MTC_auto_PRIMA_PIKE_MTC_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_MTC_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_MTC_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_MTC_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_MTC_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_MTC_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_MTC] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_MTC_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_MTC_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_MTC_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_MTC_auto]'

	---PCP
	set @sqltext=@sqltext+'  
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_PCP_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[PERFACTOR] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[PERACTIVITY] [bigint] NULL,
		[PERSTATE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	
	WITH
	(
	DATA_COMPRESSION = PAGE
	)
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_PCP_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_PCP_auto_PK_PRIMA_PIKE_PCP] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[SEQ_NO] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_PCP_auto_PRIMA_PIKE_PCP_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_PCP_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_PCP_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_PCP_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_PCP_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_PCP_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_PCP] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_PCP_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_PCP_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_PCP_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_PCP_auto]'

	---SMS_MO
	set @sqltext=@sqltext+' 
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_SMS_MO_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[ACCESSKEY] [varchar](32) COLLATE Cyrillic_General_CI_AS NULL,
		[OWNINGCUSTOMERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EVENTTYPE] [int] NULL,
		[EFFECTIVEEVENTTIME] [datetime] NULL,
		[BILLCYCLEID] [bigint] NULL,
		[ERRORCODE] [bigint] NULL,
		[RATEEVENTTYPE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EXTERNALCORRELATIONID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSCRMTITLE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGNORMALIZEDVLRDESTINATION] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGNORMALIZEDVLRORIGINATOR] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[DMTRASIAVPDATA] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDCALLED] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDCALLER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSOFFERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERUSERIMSI] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CALLREFERENCENUMBER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXMERCHANTID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXPRODUCTID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXPURPOSE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGNORMALIZEDDESTINATION] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[HTZ] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CHARGERESULT] [bigint] NULL,
		[REASONINFO] [bigint] NULL,
		[CALLSTOPTIME] [bigint] NULL,
		[CALLDURATION] [bigint] NULL,
		[DMIREQUESTSESSIONID] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[ONSUBMISSIONTIMEINMILLISECONDS] [datetime] NULL,
		[DMITICKETSEVENTCAUSE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[TOFFSET] [numeric](4, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	
	WITH
	(
	DATA_COMPRESSION = PAGE
	)
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_SMS_MO_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_SMS_MO_auto_PK_PRIMA_PIKE_SMS_MO] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_SMS_MO_auto_PRIMA_PIKE_SMS_MO_ACC_IDX] ON [dbo].[staging_PRIMA_PIKE_SMS_MO_auto]
	(
		[ACCESSKEY] ASC,
		[EVENTTYPE] ASC
	)
	INCLUDE ( 	[PARTTIME]) WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '

	--Added on 30.06.2014:
	set @sqltext=@sqltext+'
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_SMS_MO_auto_prima_pike_sms_mo_perf] ON [dbo].[staging_PRIMA_PIKE_SMS_MO_auto] 
	(
		[ACCESSKEY] ASC,
		[EFFECTIVEEVENTTIME] ASC
	)
	INCLUDE ( [CALLDURATION],
	[TOFFSET]) WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	--

	set @sqltext=@sqltext+'
	 
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_SMS_MO_auto_PRIMA_PIKE_SMS_MO_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_SMS_MO_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_SMS_MO_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_SMS_MO_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')

	ALTER TABLE [dbo].[staging_PRIMA_PIKE_SMS_MO_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_SMS_MO_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_SMS_MO] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_SMS_MO_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_SMS_MO_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_SMS_MO_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_SMS_MO_auto]'

	---SMS_MT
	set @sqltext=@sqltext+'  
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_SMS_MT_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[ACCESSKEY] [varchar](32) COLLATE Cyrillic_General_CI_AS NULL,
		[OWNINGCUSTOMERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EVENTTYPE] [int] NULL,
		[EFFECTIVEEVENTTIME] [datetime] NULL,
		[BILLCYCLEID] [bigint] NULL,
		[ERRORCODE] [bigint] NULL,
		[RATEEVENTTYPE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EXTERNALCORRELATIONID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSCRMTITLE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGNORMALIZEDVLRDESTINATION] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGNORMALIZEDVLRORIGINATOR] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[DMTRASIAVPDATA] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDCALLED] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[SS7NORMALIZEDCALLER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSOFFERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERUSERIMSI] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CALLREFERENCENUMBER] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXMERCHANTID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXPRODUCTID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXPURPOSE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGNORMALIZEDDESTINATION] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[HTZ] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CHARGERESULT] [bigint] NULL,
		[REASONINFO] [bigint] NULL,
		[CALLSTOPTIME] [bigint] NULL,
		[CALLDURATION] [bigint] NULL,
		[DMIREQUESTSESSIONID] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[ONSUBMISSIONTIMEINMILLISECONDS] [datetime] NULL,
		[DMITICKETSEVENTCAUSE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[TOFFSET] [numeric](4, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	
	WITH
	(
	DATA_COMPRESSION = PAGE
	)
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_SMS_MT_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_SMS_MT_auto_PK_PRIMA_PIKE_SMS_MT] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_SMS_MT_auto_PRIMA_PIKE_SMS_MT_ACC_IDX] ON [dbo].[staging_PRIMA_PIKE_SMS_MT_auto]
	(
		[ACCESSKEY] ASC,
		[EVENTTYPE] ASC
	)
	INCLUDE ( 	[PARTTIME]) WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '

	--Added on 30.06.2014:
	set @sqltext=@sqltext+'
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_SMS_MT_auto_prima_pike_sms_mt_perf] ON [dbo].[staging_PRIMA_PIKE_SMS_MT_auto] 
	(
		[ACCESSKEY] ASC,
		[EFFECTIVEEVENTTIME] ASC
	)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	--

	set @sqltext=@sqltext+'
	 
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_SMS_MT_auto_PRIMA_PIKE_SMS_MT_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_SMS_MT_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_SMS_MT_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_SMS_MT_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')

	ALTER TABLE [dbo].[staging_PRIMA_PIKE_SMS_MT_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_SMS_MT_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_SMS_MT] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_SMS_MT_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_SMS_MT_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_SMS_MT_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_SMS_MT_auto]'

	---VAS
	set @sqltext=@sqltext+'  
	SET ANSI_NULLS ON
	SET QUOTED_IDENTIFIER ON
	CREATE TABLE [dbo].[staging_PRIMA_PIKE_VAS_auto](
		[USEID] [bigint] NOT NULL,
		[EVENTID] [bigint] NOT NULL,
		[ACCESSKEY] [varchar](32) COLLATE Cyrillic_General_CI_AS NULL,
		[OWNINGCUSTOMERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EVENTTYPE] [int] NULL,
		[EFFECTIVEEVENTTIME] [datetime] NULL,
		[BILLCYCLEID] [bigint] NULL,
		[ERRORCODE] [bigint] NULL,
		[RATEEVENTTYPE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[EXTERNALCORRELATIONID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSCRMTITLE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXUSEDUNITS] [bigint] NULL,
		[GEXREQUESTEDUNITS] [bigint] NULL,
		[GEXRATINGAMOUNT] [bigint] NULL,
		[GEXCURRENCY] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXMERCHANTID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXPRODUCTID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[GEXPURPOSE] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGMESSAGESIZE] [bigint] NULL,
		[RATINGMESSAGETYPE] [bigint] NULL,
		[RATINGNORMALIZEDDESTINATION] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGNORMALIZEDVLRDESTINATION] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGNORMALIZEDVLRORIGINATOR] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[TINGRECIPIENTTYPEOFDESTINATION] [bigint] NULL,
		[RATINGVASID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[RATINGVASPID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CUSTOMERROPSOFFERID] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[HTZ] [varchar](1000) COLLATE Cyrillic_General_CI_AS NULL,
		[CHARGERESULT] [bigint] NULL,
		[REASONINFO] [bigint] NULL,
		[CALLSTOPTIME] [bigint] NULL,
		[CALLDURATION] [bigint] NULL,
		[DMIREQUESTSESSIONID] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[NSNPPIINFORMATIONNSNMETHODNAME] [bigint] NULL,
		[INFORMATIONNSNACCESSFRONTENDID] [varchar](4000) COLLATE Cyrillic_General_CI_AS NULL,
		[DMITICKETSEVENTCAUSE] [bigint] NULL,
		[STATUS] [bigint] NULL,
		[SEQ_NO] [numeric](38, 0) NOT NULL,
		[TOFFSET] [numeric](4, 0) NOT NULL,
		[PARTTIME] [datetime2](7) NOT NULL,
		[TRANSACTION_ID] [bigint] NULL
	) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	
	WITH
	(
	DATA_COMPRESSION = PAGE
	)
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_VAS_auto] ADD  CONSTRAINT [staging_PRIMA_PIKE_VAS_auto_PK_PRIMA_PIKE_VAS] PRIMARY KEY NONCLUSTERED 
	(
		[USEID] ASC,
		[EVENTID] ASC,
		[PARTTIME] ASC
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_VAS_auto_PRIMA_PIKE_VAS_ACC_IDX] ON [dbo].[staging_PRIMA_PIKE_VAS_auto]
	(
		[ACCESSKEY] ASC,
		[EVENTTYPE] ASC
	)
	INCLUDE ( 	[PARTTIME]) WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '

	--Added on 30.06.2014:
	set @sqltext=@sqltext+'
	CREATE NONCLUSTERED INDEX [staging_PRIMA_PIKE_VAS_auto_prima_pike_sms_vas_perf] ON [dbo].[staging_PRIMA_PIKE_VAS_auto] 
	(
		[ACCESSKEY] ASC,
		[EFFECTIVEEVENTTIME] ASC
	)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	--

	set @sqltext=@sqltext+'
	CREATE CLUSTERED INDEX [staging_PRIMA_PIKE_VAS_auto_PRIMA_PIKE_VAS_SEQ_IDX] ON [dbo].[staging_PRIMA_PIKE_VAS_auto]
	(
		[SEQ_NO] ASC,
		[PARTTIME]
	)WITH (PAD_INDEX = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON '
	set @sqltext=@sqltext+' ['+@min_fg+'] '
	set @sqltext=@sqltext+'
	 
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_VAS_auto]  WITH CHECK ADD  CONSTRAINT [chk_staging_PRIMA_PIKE_VAS_auto_partition_'+cast(@part_num as varchar(2))+'] CHECK ([PARTTIME]>='''+
	cast(@start_time as char(19))+''' AND [PARTTIME]<'''+cast(@end_time as varchar(19))+''')
	ALTER TABLE [dbo].[staging_PRIMA_PIKE_VAS_auto] CHECK CONSTRAINT [chk_staging_PRIMA_PIKE_VAS_auto_partition_'+cast(@part_num as varchar(2))+']
	ALTER TABLE [OCS].[dbo].[PRIMA_PIKE_VAS] SWITCH partition '+cast(@part_num as varchar(2))+' TO [OCS].[dbo].[staging_PRIMA_PIKE_VAS_auto]

	SELECT * INTO [OCS_'+@part_name+'_Staging].[dbo].[staging_PRIMA_PIKE_VAS_auto] FROM [OCS].[dbo].[staging_PRIMA_PIKE_VAS_auto]

	DROP TABLE [OCS].[dbo].[staging_PRIMA_PIKE_VAS_auto]'
	execute (@sqltext)
	print ('end splitting of tables at '+convert(varchar(30),getdate(),114))
	COMMIT TRANSACTION


	--Merging partition range to remove the corresponding partition from the partition scheme
	set @sqltext='ALTER PARTITION FUNCTION OCS_PART_FUNC() MERGE RANGE ('''+cast(@start_time as char(19))+''')';
	execute (@sqltext)

	set @sqltext='BACKUP DATABASE OCS_'+@part_name+'_Staging TO DISK = ''\\pmtldp01\Backups\HEAD\OCS_'+@part_name+'_Staging.bak'''
	print (@sqltext)
	execute (@sqltext)
	print ('end backing up staging db at '+convert(varchar(30),getdate(),114))

	--deletion of file of the switched out partition and corresponding filegroup
	--USE [master]
	set @sqltext='
	ALTER DATABASE [OCS] REMOVE FILE [OCS_'+@part_name+']
	ALTER DATABASE [OCS] REMOVE FILEGROUP ['+@min_fg+']'
	print (@sqltext)
	execute (@sqltext)
	print ('end of deleting partition filegroup files at '+convert(varchar(30),getdate(),114))

	END TRY


	BEGIN CATCH
		Print 'Cannot create a new staging database, switching out of the oldest partition failed!'
		SELECT 
			ERROR_NUMBER() AS ErrorNumber
			,ERROR_SEVERITY() AS ErrorSeverity
			,ERROR_STATE() AS ErrorState
			,ERROR_LINE() AS ErrorLine
			,ERROR_MESSAGE() AS ErrorMessage;
		IF @@TRANCOUNT>0 BEGIN
			set @sqltext=cast(@@TRANCOUNT as varchar(3))+' transaction(s) will be rolled back!'
			Print (@sqltext)
			ROLLBACK TRANSACTION;
		END
	END CATCH



END

GO
