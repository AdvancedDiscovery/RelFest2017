--USE EDDS1053903 -- no gaps
USE EDDS1053904 --gaps



;WITH ctrl AS
(
SELECT 
	Load = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN 'INT_X'
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			 LEFT(doc.ControlNumber, LEN(doc.ControlNumber) - PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) + 1) END
	, ControlNumber = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN CAST(doc.ControlNumber AS INT)
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT)
	END
FROM EDDSDBO.Document doc
)
SELECT Load
	, ControlNumber
	, grp = ControlNumber - ROW_NUMBER() OVER (PARTITION BY Load ORDER BY ControlNumber)
FROM ctrl


;WITH ctrl AS
(
SELECT 
	Load = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN 'INT_X'
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			 LEFT(doc.ControlNumber, LEN(doc.ControlNumber) - PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) + 1) END
	, ControlNumber = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN CAST(doc.ControlNumber AS INT)
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT)
	END
FROM EDDSDBO.Document doc
), a AS
(
SELECT Load
	, ControlNumber
	, grp = ControlNumber - ROW_NUMBER() OVER (PARTITION BY Load ORDER BY ControlNumber)
FROM ctrl
)
SELECT 
	Load
	, [Range Start] = MIN(ControlNumber)
	, [Range End] = MAX(ControlNumber) 
FROM a GROUP BY Load, grp
ORDER BY 1,2



;WITH ctrl AS
(
SELECT 
	Load = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN 'INT_X'
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			 LEFT(doc.ControlNumber, LEN(doc.ControlNumber) - PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) + 1) END
	, ControlNumber = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN CAST(doc.ControlNumber AS INT)
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT)
	END
FROM EDDSDBO.Document doc
)
SELECT d.Load, f.Seq, f.Num
   FROM (
       SELECT Load,
              ROW_NUMBER() OVER (PARTITION BY Load ORDER BY MIN(ControlNumber)) AS Grp,
              MIN(ControlNumber) AS StartSeqNo,
              MAX(ControlNumber) AS EndSeqNo
       FROM (
            SELECT Load, ControlNumber,
                   rn = ControlNumber - ROW_NUMBER() OVER (PARTITION BY Load ORDER BY ControlNumber) 
            FROM ctrl
            ) AS a
       GROUP BY Load,rn
       ) d
   CROSS APPLY (
       VALUES (d.Grp, d.EndSeqNo + 1),(d.Grp - 1, d.StartSeqNo - 1)
       ) AS f(Seq, Num)
	   ORDER BY  d.Load, Seq




;WITH ctrl AS
(
SELECT 
	Load = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN 'INT_X'
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			 LEFT(doc.ControlNumber, LEN(doc.ControlNumber) - PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) + 1) END
	--Load = (SELECT l.Name FROM EDDSDBO.Load l WHERE l.ArtifactId = doc.Load)
	, ControlNumber = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN CAST(doc.ControlNumber AS INT)
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT)
	END
FROM EDDSDBO.Document doc
), ranges AS
(
	SELECT Load
		, ControlNumber
		, grp = ControlNumber - ROW_NUMBER() OVER (PARTITION BY Load ORDER BY ControlNumber)
	FROM ctrl
)
, gaps AS
(
SELECT d.Load, f.Seq, f.ControlNumber
   FROM (
       SELECT Load,
              ROW_NUMBER() OVER (PARTITION BY Load ORDER BY MIN(ControlNumber)) AS Grp,
              MIN(ControlNumber) AS StartSeqNo,
              MAX(ControlNumber) AS EndSeqNo
       FROM ranges
       GROUP BY Load, grp
       ) d
   CROSS APPLY (
       VALUES (d.Grp, d.EndSeqNo + 1),(d.Grp - 1, d.StartSeqNo - 1)
       ) AS f(Seq, ControlNumber)
)
SELECT 
	Load
	, [Type] ='Range'
	, [Range Start] = MIN(ControlNumber)
	, [Range End] = MAX(ControlNumber) 
	, [Count] = MAX(ControlNumber) - MIN(ControlNumber) +1 
FROM ranges GROUP BY Load, grp
UNION
SELECT 
	Load
	, 'Gap'
	, [Start Gap] =  MIN(ControlNumber)
	, [End Gap] = MAX(ControlNumber)
	, [Count] = MAX(ControlNumber) - MIN(ControlNumber) +1 
FROM gaps
GROUP BY Load, Seq
HAVING COUNT(*) = 2
ORDER BY 1,3



--New using LAG
;WITH ctrl AS
(
SELECT 
	Load = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN 'INT_X'
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			 LEFT(doc.ControlNumber, LEN(doc.ControlNumber) - PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) + 1) END
	--Load = (SELECT l.Name FROM EDDSDBO.Load l WHERE l.ArtifactId = doc.Load)
	, ControlNumber = CASE WHEN TRY_CAST(doc.ControlNumber AS INT) IS NOT NULL THEN CAST(doc.ControlNumber AS INT)
		WHEN TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT) IS NOT NULL THEN
			TRY_CAST(RIGHT(doc.ControlNumber, PATINDEX( '%[^0-9]%', REVERSE( doc.ControlNumber )) - 1) AS INT)
	END
FROM EDDSDBO.Document doc
)
SELECT x.Load, x.ControlNumber, x.ControlNumber+x.gap-1, x.gap FROM (
SELECT ctrl.Load, ControlNumber = ctrl.ControlNumber+1, gap = LEAD(ctrl.ControlNumber, 1,ctrl.ControlNumber) OVER (PARTITION BY Load ORDER BY ctrl.ControlNumber)- ctrl.ControlNumber-1
FROM ctrl) x
WHERE gap > 0
