

WITH Gaps AS 
(
    SELECT ID, StartSeqNo=SeqNo + 1, EndSeqNo=(
        SELECT MIN(B.SeqNo)
        FROM dbo.GapsIslands AS B
        WHERE B.ID = A.ID AND B.SeqNo > A.SeqNo) - 1
    FROM dbo.GapsIslands AS A
    WHERE NOT EXISTS (
        SELECT *
        FROM dbo.GapsIslands AS B
        WHERE B.ID = A.ID AND B.SeqNo = A.SeqNo + 1) AND
            SeqNo < (SELECT MAX(SeqNo) 
                     FROM dbo.GapsIslands B 
                     WHERE B.ID = A.ID)
)
    ,MinMax AS 
(
    SELECT ID, MinSeqNo=MIN(SeqNo), MaxSeqNo=MAX(SeqNo)
    FROM dbo.GapsIslands
    GROUP BY ID
)
SELECT ID, MinSeqNo=MIN(SeqNo), MaxSeqNo=MAX(SeqNo)
FROM (
    SELECT ID, SeqNo, m=(ROW_NUMBER() OVER (PARTITION BY ID ORDER BY SeqNo)-1)/2
    FROM (
        SELECT ID, MinSeqNo=EndSeqNo+1, MaxSeqNo=StartSeqNo-1 
        FROM Gaps
        UNION ALL 
        SELECT ID, MinSeqNo, MaxSeqNo
        FROM MinMax) a
    CROSS APPLY (VALUES (MinSeqNo),(MaxSeqNo)) b (SeqNo)) b
GROUP BY ID, m;



;WITH cteSource(ID, Seq, Num)
AS (
   SELECT d.ID, f.Seq, f.Num
   FROM (
       SELECT ID,
              ROW_NUMBER() OVER (PARTITION BY ID ORDER BY MIN(SeqNo)) AS Grp,
              MIN(SeqNo) AS StartSeqNo,
              MAX(SeqNo) AS EndSeqNo
       FROM (
            SELECT ID, SeqNo,
                   SeqNo - ROW_NUMBER() OVER (PARTITION BY ID ORDER BY SeqNo) AS rn
            FROM dbo.GapsIslands
            ) AS a
       GROUP BY ID,rn
       ) d
   CROSS APPLY (
       VALUES (d.Grp, d.EndSeqNo + 1),(d.Grp - 1, d.StartSeqNo - 1)
       ) AS f(Seq, Num)
)
SELECT ID, MIN(Num) AS StartSeqNo, MAX(Num) AS EndSeqNo
FROM cteSource
GROUP BY ID, Seq
HAVING COUNT(*) = 2
--ORDER BY 1,2;  

SET STATISTICS IO,TIME ON;

;WITH x
AS
(
SELECT gi.ID
, SeqNo = gi.SeqNo+1
, gap = LEAD(gi.SeqNo, 1,gi.SeqNo) OVER (PARTITION BY ID ORDER BY gi.SeqNo) - gi.SeqNo 
FROM dbo.GapsIslands gi
)
SELECT x.ID, StartSeqNo = x.SeqNo, EndSeqNo = x.SeqNo+x.gap-1 
FROM x
WHERE gap > 1
--ORDER BY 1,2


GO

;WITH cteSource(ID, Seq, Num)
AS (
   SELECT d.ID, f.Seq, f.Num
   FROM (
       SELECT ID,
              ROW_NUMBER() OVER (PARTITION BY ID ORDER BY MIN(SeqNo)) AS Grp,
              MIN(SeqNo) AS StartSeqNo,
              MAX(SeqNo) AS EndSeqNo
       FROM (
            SELECT ID, SeqNo,
                   SeqNo - ROW_NUMBER() OVER (PARTITION BY ID ORDER BY SeqNo) AS rn
            FROM dbo.GapsIslands
            ) AS a
       GROUP BY ID,rn
       ) d
   CROSS APPLY (
       VALUES (d.Grp, d.EndSeqNo + 1),(d.Grp - 1, d.StartSeqNo - 1)
       ) AS f(Seq, Num)
)
SELECT ID, MIN(Num) AS StartSeqNo, MAX(Num) AS EndSeqNo
FROM cteSource
GROUP BY ID, Seq
HAVING COUNT(*) = 2;  