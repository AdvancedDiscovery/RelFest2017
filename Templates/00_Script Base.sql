
-- SQL Prompt Formatting Off
DECLARE @log TABLE (id INT PRIMARY KEY IDENTITY(1, 1) NOT NULL, field NVARCHAR(255), value SQL_VARIANT);
INSERT INTO @log VALUES ('STARTING', GETDATE())
INSERT INTO @log VALUES ('HOSTNAME', HOST_NAME()),('SERVER', @@SERVERNAME),('Database', DB_NAME());
-- SQL Prompt Formatting On

--If you have any Script variables flying in save them to the @log here.
--DECLARE @Variable SYSNAME = '#Variable#' --be warned, the script lanuage will add t-sql quotes [] around fields
--INSERT INTO @log VALUES ('#Variable#', @Variable )


BEGIN TRY
	BEGIN TRANSACTION;

	--whatever your script does, do it here


	--COMMIT TRANSACTION -- I hold off on committing the transaction until the reporting has succeeded
	ROLLBACK TRANSACTION; -- for testing remove this and uncomment the above when you are sure :)
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
	DECLARE @ErrorNumber INT = ERROR_NUMBER();
	DECLARE @ErrorLine INT = ERROR_LINE();
	DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
	DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
	DECLARE @ErrorState INT = ERROR_STATE();
-- SQL Prompt Formatting Off
	INSERT INTO @log
	VALUES
	('Error Number', @ErrorNumber)
	, ('Error Line', @ErrorLine)
	, ('Error Severity', @ErrorSeverity)
	, ('Error State', @ErrorState);
 	INSERT INTO @log ('Error Message', @ErrorMessage)
-- SQL Prompt Formatting On
END CATCH;




-- SQL Prompt Formatting Off
SELECT id, field, value FROM @log ORDER BY 1;
-- SQL Prompt Formatting On

