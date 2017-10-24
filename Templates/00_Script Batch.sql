
-- SQL Prompt Formatting Off
DECLARE @log TABLE (id INT PRIMARY KEY IDENTITY(1, 1) NOT NULL, field NVARCHAR(255), value SQL_VARIANT);
INSERT INTO @log
VALUES
('STARTING', GETDATE())
INSERT INTO @log
VALUES
('HOST', HOST_ID()),
('HOSTNAME', HOST_NAME()),
('SERVER', @@SERVERNAME),
('VERSION', @@VERSION),
('Database', DB_NAME());
-- SQL Prompt Formatting On


--Build Update using SELECT...INTO (fastest);
--This is the place for WHERE filtering, *not* the update
SELECT ArtifactID
	, TextSizeBytes = TRY_CAST(DATALENGTH( ExtractedText ) AS INT)
INTO #update
FROM EDDSDBO.Document WITH (NOLOCK)
WHERE ExtractedText IS NOT NULL
	AND ExtractedTextSize IS NULL;

--SELECT Document.ArtifactID
--INTO    #update
--#SavedSearch#


--add a clustered index/pk to our temp table
ALTER TABLE #update ADD PRIMARY KEY (ArtifactID);

--1000 is, for all intents and purposes, the best batch size.
--5000 or above will escalate to a table lock (which we don't want)
--at 1000-4999, throughput doesn't get any better, but you lock more things longer.


DECLARE @ArtifactID INT = 0;
DECLARE @pastedRows INT = 0;
DECLARE @rows INT = 1000;

BEGIN TRY
	--BEGIN TRANSACTION; --NB: no transaction here. If we get part way through the batched update and fail, we would normally like to keep the good work we have already done
	WHILE @rows = 1000
	BEGIN

		--CTE: The Common Table Expression
		--it is not strictly necessary to genericize the column names here; 
		WITH
		upd (ArtifactID, ColumnToUpdate, IncomingValue) AS
		(
			SELECT TOP 1000 --I tend to hard-code this as I've seen situations where using a variable gives you a bad plan and 99% of the time 1000 is best anyway.
				d.ArtifactID
			, d.ExtractedTextSize
			, u.TextSizeBytes
			FROM EDDSDBO.Document d
			JOIN #update u ON d.ArtifactID = u.ArtifactID
			WHERE d.ArtifactID > @ArtifactID --This is the only criteria you ever want in the WHERE clause; it keeps your UPDATES consistent with an efficient seek.
			ORDER BY d.ArtifactID --This is key.  UPDATE TOP does not produce ordered updates; encapsulating this within a CTE means we update in ascending ArtifactID order.
		)
		UPDATE upd
		SET upd.ColumnToUpdate = upd.IncomingValue
		, @ArtifactID = CASE WHEN upd.ArtifactID > @ArtifactID THEN upd.ArtifactID
							ELSE @ArtifactID
						END --after the statement, @ArtifactID will hold highest ArtifactID updated.

		OPTION(MAXDOP 1);
		--This is known as a "quirky" update.
		SET @rows = @@ROWCOUNT;
		SET @pastedRows = @pastedRows + @rows

		INSERT INTO @log VALUES ('Last Artifact', @ArtifactID), ('Row Count', @rows);
	END
	INSERT INTO @log VALUES ('Finished Correctly after', @pastedRows);
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
	DECLARE @ErrorNumber INT = ERROR_NUMBER();
	DECLARE @ErrorLine INT = ERROR_LINE();
	DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
	DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
	DECLARE @ErrorState INT = ERROR_STATE();

	INSERT INTO @log
	VALUES
	('Error Number', @ErrorNumber)
,	('Error Line', @ErrorLine)
,	('Error Message', @ErrorMessage)
,	('Error Severity', @ErrorSeverity)
,	('Error State', @ErrorState);
END CATCH;


-- SQL Prompt Formatting Off
--If we want to save the log to somewhere we do it here.
SELECT id, field, value FROM @log ORDER BY 1;
-- SQL Prompt Formatting On

