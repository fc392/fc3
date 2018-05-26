DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv, q4v;

-- Question 0
CREATE VIEW q0(era) 
AS
  SELECT era FROM pitching WHERE era IS NOT NULL order by era desc LIMIT 1
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear FROM master WHERE weight > 300 
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear FROM master WHERE namefirst ~ '.* .*' 
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  select birthyear, avg(height), count(*) from master group by birthyear having count(*) >0 order by birthyear asc
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  select birthyear, avg(height), count(*) from master group by birthyear having count(*) >0 and avg(height) > 70 order by birthyear asc
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  select namefirst, namelast, H.playerid, yearid from halloffame as H,master as M where H.playerid = M.playerid and H.inducted LIKE 'Y' order by yearid desc
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  select namefirst, namelast, M.playerid, S.schoolid, H.yearid from master as M, halloffame as H,collegeplaying as C,schools as S where H.playerid = M.playerid and M.playerid = C.playerid and C.schoolid = S.schoolid and S.schoolstate LIKE 'CA' and H.inducted LIKE 'Y' order by H.yearid desc,S.schoolid, M.playerid asc
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  select H.playerid, namefirst, namelast, S.schoolid from master as M inner join ((halloffame as H left join collegeplaying as C on H.playerid = C.playerid)  left join schools as S on C.schoolid = S.schoolid) on H.playerid = M.playerid where H.inducted LIKE 'Y' order by H.playerid desc, S.schoolid asc
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT MTable.playerid, namefirst, namelast, yearid, cast((h - h2b - h3b - hr + (2 * h2b) + (3 * h3b) + (4 * hr)) AS FLOAT) / (AB) AS slg
  FROM batting as BTable INNER JOIN master as MTable ON Btable.playerid = MTable.playerid
  WHERE BTable.AB > 50
  ORDER BY slg DESC, yearid, MTable.playerid
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT MTable.playerid, namefirst, namelast,cast(sum(h - h2b - h3b - hr + (2 * h2b) + (3 * h3b) + (4 * hr)) AS FLOAT) / sum(AB) AS lslg
  FROM batting as BTable INNER JOIN master as MTable ON Btable.playerid = MTable.playerid
  GROUP BY MTable.playerid
  HAVING sum(AB) > 50
  ORDER BY lslg DESC, MTable.playerid ASC
  LIMIT 10;
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT namefirst, namelast,cast(sum(h - h2b - h3b - hr + (2 * h2b) + (3 * h3b) + (4 * hr)) AS FLOAT) / sum(AB) AS lslg
  FROM batting as BTable INNER JOIN master as MTable ON Btable.playerid = MTable.playerid
  GROUP BY MTable.playerid
  HAVING sum(AB) > 50 and cast(sum(h - h2b - h3b - hr + (2 * h2b) + (3 * h3b) + (4 * hr)) AS FLOAT) / sum(AB) > (SELECT cast(sum(h - h2b - h3b - hr + (2 * h2b) + (3 * h3b) + (4 * hr)) AS FLOAT) / sum(AB) FROM batting WHERE playerid = 'mayswi01' )
  ORDER BY lslg DESC, MTable.playerid ASC
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
  SELECT yearid, min(salary) as min, max(salary) as max, avg(salary) as avg, stddev(salary) as stddev
FROM salaries
GROUP BY yearid
ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH minSalary AS (SELECT min(salary) AS min FROM salaries WHERE yearid = 2016),
maxSalary AS (SELECT max(salary) AS max FROM salaries WHERE yearid = 2016)

SELECT binid, (minSalary.min + binid * (maxSalary.max-minSalary.min)/10) AS low,
(minSalary.min + (binid+1) *  (maxSalary.max-minSalary.min)/10) AS high, count
FROM(
SELECT  FLOOR((salary - minSalary.min)/(maxSalary.max+1 - minSalary.min)*10) AS binid, count(*)
FROM salaries,minSalary, maxSalary
WHERE yearid = 2016
GROUP BY Floor((salary - minSalary.min)/(maxSalary.max+1 - minSalary.min)*10)
) AS sub, minSalary, maxSalary
ORDER BY binid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH temp AS (SELECT yearid,min(salary) AS minS, max(salary) AS maxS, avg(salary) AS avgS
FROM salaries
GROUP BY yearid
)

SELECT A.yearid, A.minS-B.minS as mindiff, A.maxS-B.maxS as maxdiff, A.avgS-B.avgS As avgdiff
FROM temp as A, temp as B
WHERE A.yearid - 1 = B.yearid
ORDER BY A.yearid asc
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT master.playerid, namefirst, namelast, salary, yearid
FROM salaries INNER JOIN master on salaries.playerid = master.playerid
WHERE (salary,yearid) in
(SELECT max(salary),yearid
FROM salaries
WHERE yearid = 2001 OR yearid = 2000
GROUP BY yearid
)
;

;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
SELECT A.teamid, max(salary)-min(salary) AS diffAvg
FROM salaries AS S INNER JOIN allstarfull AS A
ON S.yearid = A.yearid
AND S.playerid = A.playerid
WHERE A.yearid = 2016
GROUP BY A.teamid
ORDER BY A.teamid
;

;

