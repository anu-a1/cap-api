USE [LawsonDW]
GO
/****** Object:  StoredProcedure [CAP].[CREATE_LOG]    Script Date: 8/1/2017 10:51:24 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROC [CAP].[CREATE_LOG]
@RUN_ID INT
,@PROCESS VARCHAR(150)
,@DESCRIPTION VARCHAR(4000)
,@ROWS_AFFECTED INT = NULL
,@IS_ERROR VARCHAR(10)  = NULL
,@START_DATE DATETIME  = NULL
,@END_DATE DATETIME  = NULL
,@CREATED_BY VARCHAR(50)  = NULL
,@FOR_UI BIT = 0
,@LOG_ID INT OUTPUT
AS
BEGIN

    INSERT INTO CAP.LOGS
           (
			 [RUN_ID]
			,[PROCESS]
			,[DESCRIPTION]
			,[ROWS_AFFECTED]
			,[IS_ERROR]
			,[START_DATE]
			,[END_DATE]
			,[CREATED_BY]
			,[FOR_UI]
		 )
     VALUES
           (
			  @RUN_ID
			 ,@PROCESS
			 ,@DESCRIPTION 
			 ,@ROWS_AFFECTED
			 ,@IS_ERROR
			 ,@START_DATE 
			 ,@END_DATE
			 ,@CREATED_BY 
			 ,@FOR_UI
		 )

		 SET @LOG_ID = SCOPE_IDENTITY()


END

GO
/****** Object:  StoredProcedure [CAP].[FIGGEN_ALLOCATIONPROCESS]    Script Date: 8/1/2017 10:51:24 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROC [CAP].[FIGGEN_ALLOCATIONPROCESS]
AS
/***************************************************************************
Sample run: EXEC [CAP].[FIGGEN_ALLOCATIONPROCESS] 

***************************************************************************/
BEGIN

    SET NOCOUNT ON

	DECLARE @RUNGROUP VARCHAR(15)= ''
	,@FISCAL_YEAR INT 
	,@PERIOD INT 
	,@ALLOC_MDL VARCHAR(32) = 'FIGGEN'
	,@RETURNED_LOG_ID int = 1
	,@RETURNED_PROCESS_LOG_ID int = 1
	,@START_DATE DATETIME = SYSDATETIME()
	,@END_DATE DATETIME
	,@PROCESS VARCHAR(100) = (SELECT OBJECT_NAME(@@PROCID))

    BEGIN TRY
	   BEGIN TRAN

		  SELECT @PERIOD = CNTL_NUM1, @FISCAL_YEAR = CNTL_NUM2
		  FROM L_CNTL_CONTROL
		  WHERE CNTL_KEY1 = 'LAWSON CAP' AND CNTL_KEY2 = 'Job Parameter' AND CNTL_CHAR1 = @ALLOC_MDL

		  DECLARE @RUN_ID INT
		  SET @RUN_ID = (SELECT ISNULL(MAX(RUN_ID),0)+1 FROM CAP.LOGS)

		  EXEC [CAP].[CREATE_LOG] @RUN_ID = @RUN_ID,@PROCESS = @PROCESS ,@DESCRIPTION = N'Starting Figen Allocation Process',@ROWS_AFFECTED = 0
				,@IS_ERROR = @@ERROR ,@START_DATE = @START_DATE ,@END_DATE = NULL,@CREATED_BY = N'AACHURI',@FOR_UI = 1
				,@LOG_ID = @RETURNED_PROCESS_LOG_ID OUTPUT

		  --This proc loads the allocation percentages
		  EXEC LAWSONDW.CAP.CALC_ALLOC_PERCENTAGES @ALLOC_MDL,@FISCAL_YEAR,@PERIOD,@MAXRUNGROUP = @RUNGROUP OUTPUT

		  --DROP TEMP TABLES
		    IF EXISTS(  SELECT NULL
									   FROM tempdb.dbo.sysobjects
									   WHERE ID = OBJECT_ID(N'tempdb..#TMP'))
			BEGIN 
										 DROP TABLE #TMP 
			END
	  
			--TEMP1
			IF EXISTS(  SELECT NULL
									   FROM tempdb.dbo.sysobjects
									   WHERE ID = OBJECT_ID(N'tempdb..#TMP1'))
			BEGIN 
										 DROP TABLE #TMP1 
			END
	  
			---TEMP2
			IF EXISTS(  SELECT NULL
									   FROM tempdb.dbo.sysobjects
									   WHERE ID = OBJECT_ID(N'tempdb..#TMP2'))
			BEGIN 
										 DROP TABLE #TMP2 
			END
    
		    ---TEMP3
		    IF EXISTS(  SELECT NULL
									   FROM tempdb.dbo.sysobjects
									   WHERE ID = OBJECT_ID(N'tempdb..#TMP3'))
			BEGIN 
										 DROP TABLE #TMP3 
			END 
	  
			 ---TEMP4
		    IF EXISTS(  SELECT NULL
									   FROM tempdb.dbo.sysobjects
									   WHERE ID = OBJECT_ID(N'tempdb..#TMP4'))
			BEGIN 
										 DROP TABLE #TMP4
			END
	  
   
 
		    SELECT ALLOC_NAME,PT_ALLOC_CODE,PF_ACCT_UNIT,PF_ACCOUNT,PF_SUB_ACCT,ACCT_UNIT_01,JE_DESC,LINE_NBR,S.CURRENCY_CODE,V.*
		    INTO #TMP
			FROM LAWPROD.[dbo].RWGRPDET A
			    INNER JOIN LAWPROD.[dbo].CADETAIL B ON (A.HEAD_NAME = B.SUB_ACCT_GRP )
			    INNER JOIN LAWPROD.[dbo].RWGRPDET C ON (C.HEAD_NAME = B.ACCOUNT_GROUP )
			    INNER JOIN [LAWPROD].[dbo].[vw_GLAMOUNTS] V ON (V.ACCT_UNIT = B.ACCT_UNIT_01 and V.COMPANY = B.PT_COMPANY )
			    INNER JOIN [LAWPROD].dbo.GLSYSTEM S ON S.COMPANY = V.COMPANY
			    INNER JOIN [LawsonDW].[dbo].[L_CNTL_CONTROL] CNTL ON ALLOCATION_GRP = CNTL.[CNTL_CHAR2] AND CNTL.CNTL_KEY1 = 'LAWSON CAP' AND CNTL.CNTL_KEY2 = 'ALLOC GRP MAPPING'
			WHERE V.FISCAL_YEAR = @FISCAL_YEAR 
			AND CNTL.[CNTL_CHAR1] = @ALLOC_MDL   
				 AND (V.ACCOUNT >= C.FR_ACCOUNT AND V.ACCOUNT <= C.TH_ACCOUNT AND V.SUB_ACCOUNT >=A.FR_SUB_ACCT AND V.SUB_ACCOUNT <= A.TH_SUB_ACCT )
		    ORDER BY V.COMPANY,V.ACCT_UNIT,V.ACCOUNT		   
	  
    		 --GENERATE GL CREDIT RECORDS (NEGATIVE NUMBERS) 
		    SELECT A.COMPANY AS COMPANY, ALLOC_NAME,PT_ALLOC_CODE,LINE_NBR,PF_ACCT_UNIT,ACCOUNT, SUB_ACCOUNT,CURRENCY_CODE,JE_DESC
			 ,(SUM(AMOUNT_01) * -1) AS AMOUNT_01,(SUM(AMOUNT_02)* -1) AS AMOUNT_02,(SUM(AMOUNT_03) * -1) AS AMOUNT_03,(SUM(AMOUNT_04)* -1) AS AMOUNT_04,(SUM(AMOUNT_05)* -1) AS AMOUNT_05,(SUM(AMOUNT_06)* -1) AS AMOUNT_06,(SUM(AMOUNT_07)* -1) AS AMOUNT_07,(SUM(AMOUNT_08)* -1) AS AMOUNT_08,(SUM(AMOUNT_09)* -1) AS AMOUNT_09,(SUM(AMOUNT_10)* -1) AS AMOUNT_10,(SUM(AMOUNT_11)* -1) AS AMOUNT_11,(SUM(AMOUNT_12)* -1) AS AMOUNT_12
		    INTO #TMP1
		    FROM #TMP T 
		    LEFT OUTER JOIN (SELECT DISTINCT ALLOC_CODE, COMPANY FROM LAWPROD.[dbo].ACDETAIL) A ON (A.ALLOC_CODE = T.PT_ALLOC_CODE )
		    GROUP BY  A.COMPANY,ALLOC_NAME, PT_ALLOC_CODE,LINE_NBR,PF_ACCT_UNIT,ACCOUNT,SUB_ACCOUNT,CURRENCY_CODE,JE_DESC
   			 
		    --GET ALLOCATION DETAILS (DEBIT RECORDS)
		    ;WITH CTE AS
		    (
			   SELECT T.COMPANY, T.PT_ALLOC_CODE, T.ALLOC_NAME,LINE_NBR, PF_ACCT_UNIT, ACCOUNT,SUB_ACCOUNT,CURRENCY_CODE,JE_DESC,SUM(R_VALUE) AS SUMVALUE 
			   FROM #TMP1 T
			   INNER JOIN LAWPROD.[dbo].ACDETAIL A ON (A.COMPANY = T.COMPANY and T.PT_ALLOC_CODE = A.ALLOC_CODE)
			   GROUP BY  T.COMPANY,PT_ALLOC_CODE, PF_ACCT_UNIT, T.ALLOC_NAME, LINE_NBR,ACCOUNT,SUB_ACCOUNT,CURRENCY_CODE,JE_DESC
		    )
		    SELECT A.COMPANY
				 ,C.ALLOC_NAME,
				 T.PT_ALLOC_CODE,
				 T.LINE_NBR,
				 A.ACCT_UNIT,
				 T.ACCOUNT,
				 T.SUB_ACCOUNT
				 ,C.CURRENCY_CODE
				 ,C.JE_DESC,
				  CAST((AMOUNT_01 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_01,
				  CAST((AMOUNT_02 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_02,
				  CAST((AMOUNT_03 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_03,
				  CAST((AMOUNT_04 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_04,
				  CAST((AMOUNT_05 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_05,
				  CAST((AMOUNT_06 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_06,
				  CAST((AMOUNT_07 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_07,
				  CAST((AMOUNT_08 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_08,
				  CAST((AMOUNT_09 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_09,
				  CAST((AMOUNT_10 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_10,
				  CAST((AMOUNT_11 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_11,
				  CAST((AMOUNT_12 * -1) * (CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10))) AS DECIMAL(18,2)) AS AMOUNT_12,
				  CAST((R_VALUE/SUMVALUE)  AS DECIMAL(18,10)) AS PERCENTVALUE	         
		    INTO #TMP2      
		    FROM CTE C 
		    INNER JOIN #TMP1 T ON (C.COMPANY = T.COMPANY  and C.ALLOC_NAME = T.ALLOC_NAME 
		    and C.PT_ALLOC_CODE = T.PT_ALLOC_CODE  and C.PF_ACCT_UNIT = T.PF_ACCT_UNIT and  C.LINE_NBR = T.LINE_NBR
		    and C.ACCOUNT = T.ACCOUNT and C.SUB_ACCOUNT = T.SUB_ACCOUNT)
		    INNER JOIN LAWPROD.[dbo].ACDETAIL A ON (A.COMPANY = T.COMPANY AND A.ALLOC_CODE = T.PT_ALLOC_CODE )
		    WHERE SUMVALUE <> 0	
	         
		   ;WITH CTE AS
		    (
			   SELECT COMPANY,LINE_NBR,ALLOC_NAME,PT_ALLOC_CODE,PF_ACCT_UNIT AS ACCT_UNIT,ACCOUNT,SUB_ACCOUNT,CURRENCY_CODE,JE_DESC,AMOUNT_01,AMOUNT_02,AMOUNT_03,AMOUNT_04,AMOUNT_05,AMOUNT_06,AMOUNT_07,AMOUNT_08,AMOUNT_09,AMOUNT_10,AMOUNT_11,AMOUNT_12,1 AS PERCENTVALUE,'Credit' TRAN_TYPE
			   FROM #TMP1
			   UNION ALL
			   SELECT COMPANY,LINE_NBR,ALLOC_NAME,PT_ALLOC_CODE,ACCT_UNIT ,ACCOUNT,SUB_ACCOUNT,CURRENCY_CODE,JE_DESC,AMOUNT_01,AMOUNT_02,AMOUNT_03,AMOUNT_04,AMOUNT_05,AMOUNT_06,AMOUNT_07,AMOUNT_08,AMOUNT_09,AMOUNT_10,AMOUNT_11,AMOUNT_12,PERCENTVALUE,'Debit' TRAN_TYPE
			   FROM #TMP2
		    )
		    SELECT *
		    INTO #TMP3
		    FROM CTE
		    ORDER BY LINE_NBR	   

   		   --RETURN THE FINAL RESULT	  
		   ;WITH CTE AS    
		   (
			   SELECT 
				   COMPANY
				   ,ALLOC_NAME
				   ,@FISCAL_YEAR AS [YEAR]
				   ,@PERIOD AS PERIOD
				   ,'CA' AS SYSTEM
				   ,'N' AS JE_TYPE
				   ,'' AS CONTROL_GROUP
				   ,'' AS JE_SEQUENCE
				   ,'A' AS LINE_FC
				   ,COMPANY AS TO_COMPANY
				   ,ACCT_UNIT
				   ,C.ACCOUNT
				   ,C.SUB_ACCOUNT 
				   ,CASE WHEN @PERIOD = 1 THEN AMOUNT_01
					    WHEN @PERIOD = 2 THEN AMOUNT_02 
					    WHEN @PERIOD = 3 THEN AMOUNT_03
					    WHEN @PERIOD = 4 THEN AMOUNT_04
					    WHEN @PERIOD = 5 THEN AMOUNT_05
					    WHEN @PERIOD = 6 THEN AMOUNT_06 
					    WHEN @PERIOD = 7 THEN AMOUNT_07
					    WHEN @PERIOD = 8 THEN AMOUNT_08  
					    WHEN @PERIOD = 9 THEN AMOUNT_09
					    WHEN @PERIOD = 10 THEN AMOUNT_10 
					    WHEN @PERIOD = 11 THEN AMOUNT_11
					    WHEN @PERIOD = 12 THEN AMOUNT_12
				    END AS TRAN_AMOUNT
				   ,JE_DESC AS [DESCRIPTION]
				   ,C.CURRENCY_CODE
				   ,CASE WHEN @PERIOD = 1 THEN AMOUNT_01
					    WHEN @PERIOD = 2 THEN AMOUNT_02 
					    WHEN @PERIOD = 3 THEN AMOUNT_03
					    WHEN @PERIOD = 4 THEN AMOUNT_04
					    WHEN @PERIOD = 5 THEN AMOUNT_05
					    WHEN @PERIOD = 6 THEN AMOUNT_06 
					    WHEN @PERIOD = 7 THEN AMOUNT_07
					    WHEN @PERIOD = 8 THEN AMOUNT_08  
					    WHEN @PERIOD = 9 THEN AMOUNT_09
					    WHEN @PERIOD = 10 THEN AMOUNT_10 
					    WHEN @PERIOD = 11 THEN AMOUNT_11
					    WHEN @PERIOD = 12 THEN AMOUNT_12
				    END AS BASE_AMOUNT
				   ,0 AS RPT_AMOUNT_1
				   ,PERCENTVALUE AS ATTRIBUTE_1
				   ,'FIGGEN ALLOCATIONS' AS REFERENCE
				   ,LINE_NBR AS ATTRIBUTE_2
				   ,'' AS ATTRIBUTE3
				   ,PT_ALLOC_CODE		
				   ,TRAN_TYPE	   
			   FROM #TMP3 C			        
		    )	
		    SELECT * , CAST(0.0 AS DECIMAL(18,2)) AS ADJ_AMOUNT
		    , ROW_NUMBER() OVER(PARTITION BY COMPANY,ALLOC_NAME,ACCOUNT, SUB_ACCOUNT,ATTRIBUTE_2 ORDER BY TRAN_TYPE DESC,TRAN_AMOUNT DESC) AS RN	
		    INTO #TMP4	
		    FROM CTE
		    ORDER BY ATTRIBUTE_2 
		
		    ;WITH CTE AS
		    (
			 SELECT COMPANY,ALLOC_NAME,ACCOUNT, SUB_ACCOUNT,ATTRIBUTE_2, SUM(TRAN_AMOUNT) TOTAL_TRAN_AMOUNT
			 FROM #TMP4
			 GROUP BY COMPANY,ALLOC_NAME,ACCOUNT, SUB_ACCOUNT,ATTRIBUTE_2
			 HAVING SUM(TRAN_AMOUNT) <> 0
		    )
		    UPDATE  T 
		    SET T.TRAN_AMOUNT = T.TRAN_AMOUNT - C.TOTAL_TRAN_AMOUNT
		    ,ADJ_AMOUNT = C.TOTAL_TRAN_AMOUNT
		    FROM CTE C
		    INNER JOIN #TMP4 T 
		    ON C.ATTRIBUTE_2 = T.ATTRIBUTE_2		
			 AND C.ALLOC_NAME = T.ALLOC_NAME
			 AND C.ACCOUNT = T.ACCOUNT
			 AND C.SUB_ACCOUNT = T.SUB_ACCOUNT
			 AND C.COMPANY = T.COMPANY
		    WHERE T.RN = 1 AND TRAN_TYPE = 'Debit'		    
		
		    DECLARE @LATEST_RUNCYLE INT
		    SET @LATEST_RUNCYLE = (SELECT CAST(RIGHT(@RUNGROUP,2) AS INT))	

			--Load reversals
		    EXEC CAP.LOAD_REVERSALS @RUNGROUP

			EXEC [CAP].[CREATE_LOG] @RUN_ID = @RUN_ID,@PROCESS = @PROCESS ,@DESCRIPTION = N'Loading data to CAP.ALLOC_STAGE ',@ROWS_AFFECTED = 0
			  ,@IS_ERROR = @@ERROR ,@START_DATE = @START_DATE ,@END_DATE = NULL,@CREATED_BY = N'AACHURI'
			  ,@LOG_ID = @RETURNED_LOG_ID OUTPUT
	 
		    INSERT INTO [CAP].[ALLOC_STAGE]
			  ([RUN_GROUP]
			  ,[COMPANY]
			  ,[ALLOC_NAME]
			  ,[ACCT_UNIT]
			  ,[ACCOUNT]
			  ,[SUB_ACCOUNT]
			  ,[DESCRIPTION]
			  ,[CURRENCY_CODE]
			  ,[ALLOC_AMOUNT]
			  ,[POSTING_DATE]
			  ,[ALLOC_ID]
			  ,[ALLOC_CODE]
			  ,[ALLOC_MDL]
			  ,[ALLOC_PERCENTAGE]
			  ,[RUN_CYCLE]
			  ,[LINE_NBR]
			  ,[ACTIVITY]
			  ,[SYSTEM]
			  ,[ACCT_CATEGORY]
			  ,[ADJ_AMOUNT]		   
			  ,[CREATE_DATE]
			  ,[CREATE_TIME]
			  ,[CREATED_BY])	
		    SELECT  LTRIM(RTRIM(@ALLOC_MDL)) + LTRIM(RTRIM(CAST(T.[YEAR] AS CHAR(4))))+ LEFT('0'+LTRIM(RTRIM(CAST(T.PERIOD AS CHAR(2)))),2)+ LEFT('0'+LTRIM(RTRIM(CAST(@LATEST_RUNCYLE AS CHAR(2)))),2) AS RUN_GROUP
				   , T.COMPANY
				   , T.ALLOC_NAME
				   , T.ACCT_UNIT
				   , T.ACCOUNT
				   , T.SUB_ACCOUNT
				   , LEFT(T.[DESCRIPTION],30)
				   , T.CURRENCY_CODE
				   , T.TRAN_AMOUNT ALLOC_AMOUNT
				   , DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,CAST(CAST(@PERIOD AS VARCHAR(2))+'/'+'1'+'/' +CAST(@FISCAL_YEAR AS CHAR(4)) AS DATE))+1,0)) POSTING_DATE
				   , ISNULL(AP.ALLOC_ID,'')
				   , ISNULL(AP.ALLOC_CODE,'')
				   , @ALLOC_MDL 
				   , ISNULL(AP.ALLOC_PERCENTAGE,0)
				   , @LATEST_RUNCYLE RUN_CYCLE 
				   , T.ATTRIBUTE_2 LINE_NBR
				   , '' ACTIVITY
				   , T.SYSTEM
				   , '' ACCT_CATEGORY
				   , T.ADJ_AMOUNT
				   , CONVERT (DATE, SYSDATETIME())  CREATE_DATE
				   , CONVERT (TIME, SYSDATETIME())  CREATE_TIME
				   , SYSTEM_USER CREATED_BY
		    FROM #TMP4 T
		    LEFT JOIN  [LawsonDW].[CAP].[ALLOC_PERCENTAGES] AP on T.COMPANY=AP.COMPANY and T.PT_ALLOC_CODE = AP.ALLOC_CODE and AP.RUN_CYCLE = @LATEST_RUNCYLE
		    AND T.[YEAR] = AP.FISCAL_YEAR and T.ACCT_UNIT = AP.ACCT_UNIT and @ALLOC_MDL = AP.ALLOC_MDL AND T.PERIOD = AP.PERIOD
		    WHERE T.TRAN_AMOUNT <> 0	

			EXEC [CAP].[UPDATE_LOG] @LOG_ID = @RETURNED_LOG_ID,@ROWS_AFFECTED = @@ROWCOUNT ,@IS_ERROR = @@ERROR
			
		  -- This loads the calculations into GLTRANSREL
		  EXEC [CAP].[GLINTERFACE] @RUNGROUP

	   COMMIT TRAN

	   	EXEC [CAP].[CREATE_LOG] @RUN_ID = @RUN_ID,@PROCESS = @PROCESS ,@DESCRIPTION = N'Ending Figen Allocation Process',@ROWS_AFFECTED = 0
		,@IS_ERROR = @@ERROR ,@START_DATE = @START_DATE ,@END_DATE = NULL,@CREATED_BY = N'AACHURI',@FOR_UI = 1
		,@LOG_ID = @RETURNED_PROCESS_LOG_ID OUTPUT


		EXEC [CAP].[UPDATE_LOG] @LOG_ID = @RETURNED_PROCESS_LOG_ID,@ROWS_AFFECTED = @@ROWCOUNT ,@IS_ERROR = @@ERROR


    END TRY
    BEGIN CATCH

	   IF @@TRANCOUNT > 0
	   ROLLBACK TRANSACTION;
 
	   DECLARE @ERRORNUMBER INT = ERROR_NUMBER();
	   DECLARE @ERRORLINE INT = ERROR_LINE();
	   DECLARE @ERRORMESSAGE NVARCHAR(4000) = ERROR_MESSAGE();
	   DECLARE @ERRORSEVERITY INT = ERROR_SEVERITY();
	   DECLARE @ERRORSTATE INT = ERROR_STATE();
	   DECLARE @EMAIL_RECIPIENTS varchar(500)

	   SET @ERRORMESSAGE = @ERRORMESSAGE + ' in process '+ OBJECT_NAME(@@PROCID) + ': Line Number ' + CAST(@ERRORLINE AS VARCHAR(4))

	   SET @EMAIL_RECIPIENTS = (SELECT ISNULL(CNTL_CHAR5,'Group_LawsonAdmins@fortress.com') 
								           FROM [LawsonDW].[dbo].[L_CNTL_CONTROL] 
							               WHERE [CNTL_KEY1] = 'LAWSON CAP' 
								              AND [CNTL_KEY2] = 'Email Parameters')

	   EXEC MSDB.DBO.SP_SEND_DBMAIL @PROFILE_NAME='LawsonCube',
					   @RECIPIENTS='AACHURI@FORTRESS.COM',
					   @SUBJECT='ATTENTION: Lawson CAP process failed.',
					   @BODY = @ERRORMESSAGE,
					   @BODY_FORMAT ='HTML',
					   @IMPORTANCE='HIGH'; 
 
	   SELECT @ERRORMESSAGE ERRORMESSAGE
	   RAISERROR(@ERRORMESSAGE, @ERRORSEVERITY, @ERRORSTATE);
    END CATCH

END


GO
/****** Object:  StoredProcedure [CAP].[UPDATE_LOG]    Script Date: 8/1/2017 10:51:24 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [CAP].[UPDATE_LOG]
@LOG_ID INT 
,@ROWS_AFFECTED INT = NULL
,@IS_ERROR VARCHAR(10)  = NULL

AS
BEGIN

    DECLARE @START_DATE DATETIME  = (SELECT [START_DATE] FROM CAP.LOGS WHERE LOG_ID = @LOG_ID)
    DECLARE @DATE_DIFFERENCE int =  DATEDIFF(ms, @START_DATE, SYSDATETIME())
    DECLARE @FORMATTED_DATE_DIFFERENCE VARCHAR(50)

    SELECT @FORMATTED_DATE_DIFFERENCE = 
	   (
		  CONVERT(VARCHAR(10), (@DATE_DIFFERENCE/86400000)) + ' Day ' + 
		  CONVERT(VARCHAR(10), ((@DATE_DIFFERENCE%86400000)/3600000)) + ' Hour ' +
		  CONVERT(VARCHAR(10), (((@DATE_DIFFERENCE%86400000)%3600000)/60000)) + ' Min ' +
		  CONVERT(VARCHAR(10), ((((@DATE_DIFFERENCE%86400000)%3600000)%60000)/1000)) + ' Sec ' +
		  CONVERT(VARCHAR(10), (((@DATE_DIFFERENCE%86400000)%3600000)%1000)) + ' MS' 
	   )

    UPDATE CAP.LOGS
    SET [ROWS_AFFECTED] = @ROWS_AFFECTED
    , IS_ERROR = @IS_ERROR
    , END_DATE = SYSDATETIME()
    , [DURATION] = @FORMATTED_DATE_DIFFERENCE
    WHERE LOG_ID = @LOG_ID




END
GO
/****** Object:  Table [CAP].[LOGS]    Script Date: 8/1/2017 10:51:24 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [CAP].[LOGS](
	[LOG_ID] [int] IDENTITY(1,1) NOT NULL,
	[RUN_ID] [int] NOT NULL,
	[PROCESS] [varchar](150) NULL,
	[DESCRIPTION] [varchar](4000) NULL,
	[ROWS_AFFECTED] [int] NULL,
	[IS_ERROR] [int] NULL,
	[START_DATE] [datetime] NULL,
	[END_DATE] [datetime] NULL,
	[DURATION] [varchar](100) NULL,
	[CREATED_BY] [varchar](50) NULL,
	[FOR_UI] [bit] NOT NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [CAP].[LOGS] ADD  DEFAULT ((0)) FOR [FOR_UI]
GO
