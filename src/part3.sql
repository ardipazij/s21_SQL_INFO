--1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
CREATE OR REPLACE FUNCTION fnc_retTransPoints()
RETURNS TABLE(
    Peer1 VARCHAR,
    Peer2 VARCHAR,
    PointsAmount INT
) AS
$$
BEGIN
    RETURN query
    SELECT DISTINCT tp1.checking_peer, tp1.checked_peer, COALESCE(tp1.points_amount - tp2.points_amount, tp1.points_amount)
    FROM transferred_points tp1
    LEFT JOIN transferred_points tp2 ON tp1.checking_peer = tp2.checked_peer AND tp1.checked_peer = tp2.checking_peer;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_retTransPoints();

--2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
CREATE OR REPLACE FUNCTION fnc_retxp()
RETURNS TABLE(
    Peer VARCHAR,
    Task VARCHAR,
    XP INT
) AS
$$
BEGIN
    RETURN query
    SELECT c.peer, c.task, xp.xp_amount
    FROM checks c
    JOIN xp ON c.id = xp."check"
    JOIN p2p ON c.id = p2p.check AND p2p.state = 'success';
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_retxp();

--3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
CREATE OR REPLACE FUNCTION fnc_retTime(pdate DATE)
RETURNS TABLE(
    Peer VARCHAR
) AS
$$
BEGIN
    RETURN query
    SELECT tm.peer
    FROM time_tracking tm
    WHERE tm.data = pdate and state = 1
    EXCEPT
    SELECT tm.peer
    FROM time_tracking tm
    WHERE tm.data = pdate and state = 2;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_retTime('2023-12-02');

--4) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
CREATE OR REPLACE FUNCTION fnc_retPointsChangeTP()
RETURNS TABLE(
    Peer VARCHAR,
    PointsChange BIGINT
) AS
$$
BEGIN
    RETURN query
    WITH t AS(
        SELECT nickname, COALESCE(SUM(points_amount), 0) AS gets
        FROM transferred_points
        RIGHT JOIN peers ON transferred_points.checking_peer = peers.nickname
        GROUP BY nickname
    ),
    t1 AS (
        SELECT nickname, COALESCE(SUM(points_amount), 0) AS gives
        FROM transferred_points
        RIGHT JOIN peers ON transferred_points.checked_peer = peers.nickname
        GROUP BY nickname
    )
    SELECT t.nickname, gets-gives
    FROM t
    JOIN t1 ON t.nickname = t1.nickname;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_retPointsChangeTP();

--5) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3
CREATE OR REPLACE FUNCTION fnc_retPointsChange()
RETURNS TABLE(
    Peer VARCHAR,
    PointsChange BIGINT
) AS
$$
BEGIN
    RETURN query
    WITH checking AS (
        SELECT Peer1, SUM(PointsAmount)
        FROM fnc_retTransPoints()
        GROUP BY Peer1
    ),
    checked AS (
        SELECT Peer2, SUM(PointsAmount)
        FROM fnc_retTransPoints()
        GROUP BY Peer2
    )
    SELECT COALESCE(checked.Peer2, checking.Peer1), (COALESCE(checking.sum, 0) - COALESCE(checked.sum, 0))
    FROM checking
    FULL JOIN checked ON checking.Peer1 = checked.Peer2;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_retPointsChange();

--6) Определить самое часто проверяемое задание за каждый день
CREATE OR REPLACE FUNCTION fnc_retCountTask()
RETURNS TABLE(
    Day DATE,
    Task VARCHAR
) AS
$$
BEGIN
    RETURN query
    WITH t AS (
        SELECT ch.data, ch.task, (SELECT COUNT(c.task)
                            FROM checks c
                            WHERE c.task = ch.task AND c.data = ch.data) AS A
        FROM checks ch
    )
    SELECT DISTINCT t1.data, t1.task
    FROM t t1
    WHERE A = (SELECT MAX(A) FROM t t2 WHERE t1.data = t2.data)
    ORDER BY data DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_retCountTask();

-- 7) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
CREATE OR REPLACE FUNCTION fnc_getPeers(pname VARCHAR)
RETURNS TABLE(
    Peer VARCHAR,
    Day DATE
) AS
$$
BEGIN
    RETURN query
    WITH t AS (
        SELECT title
        FROM tasks
        WHERE substring(title FROM '.+?(?=\d{1,2})') = pname
    )
    SELECT DISTINCT checks.peer, MAX(checks.data) AS day
    FROM checks
    JOIN xp ON xp.check = checks.id
    WHERE checks.task IN (SELECT title FROM t)
    GROUP BY checks.peer
	HAVING (COUNT(DISTINCT checks.task) = (SELECT COUNT(*) FROM t))
    ORDER BY day;
END;
$$ LANGUAGE plpgsql;

-- 8) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
CREATE OR REPLACE FUNCTION fnc_getPeers()
RETURNS TABLE(
    Peer VARCHAR,
    RecommendedPeer VARCHAR
) AS
$$
BEGIN
    RETURN query
    WITH t1 AS (
        SELECT rec.peer, rec.recommended_peer, COUNT(*) AS c
        FROM recommendations rec
        GROUP BY rec.peer, rec.recommended_peer
    )
    SELECT DISTINCT r.peer, f.peer_2
    FROM friends f
    JOIN (
        SELECT t1.peer, t1.recommended_peer, c, ROW_NUMBER() OVER(PARTITION BY t1.peer ORDER BY c DESC) AS rank
        FROM t1
		JOIN recommendations ON t1.recommended_peer = recommendations.recommended_peer
    ) r ON f.peer_1 = r.recommended_peer AND r.rank = 1
	WHERE r.peer != f.peer_2;

END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_getPeers();

-- 9) Определить процент пиров

CREATE OR REPLACE FUNCTION fnc_getProgress(first_task VARCHAR, second_task VARCHAR)
RETURNS TABLE (
    StartedBlock1 BIGINT,
    StartedBlock2 BIGINT,
    StartedBothBlocks BIGINT,
    DidntStartAnyBlock BIGINT
) AS
$$
DECLARE
    c BIGINT;
BEGIN
	c = (SELECT COUNT(*) FROM peers);
    RETURN query

    WITH one AS(
        SELECT DISTINCT peer
        FROM checks
        WHERE substring(task FROM '.+?(?=\d{1,2})') = first_task
    ),
    two AS(
        SELECT DISTINCT peer
        FROM checks
        WHERE substring(task FROM '.+?(?=\d{1,2})') = second_task
    ),
    only_first AS (
        SELECT COUNT(nickname)
        FROM peers
        WHERE nickname IN (SELECT peer FROM one) AND nickname NOT IN (SELECT peer FROM two)
    ),
    only_second AS (
        SELECT COUNT(nickname)
        FROM peers
        WHERE nickname NOT IN (SELECT peer FROM one) AND nickname IN (SELECT peer FROM two)
    ),
    both_tasks AS (
        SELECT COUNT(nickname)
        FROM peers
        WHERE nickname IN (SELECT peer FROM one) AND nickname IN (SELECT peer FROM two)
    ),
    nan AS(
        SELECT COUNT(nickname)
        FROM peers
        WHERE nickname NOT IN (SELECT peer FROM one) AND nickname NOT IN (SELECT peer FROM two)
    )
    SELECT only_first.count * 100 / c, only_second.count * 100 / c, both_tasks.count * 100 / c, nan.count * 100 / c
    FROM only_first
    CROSS JOIN only_second
    CROSS JOIN both_tasks
    CROSS JOIN nan;

END;
$$ LANGUAGE plpgsql;

-- 10) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения

CREATE OR REPLACE FUNCTION fnc_getPercent()
RETURNS TABLE(
    SuccessfulChecks BIGINT,
    UnsuccessfulChecks BIGINT
) AS
$$
DECLARE
    cnt int := (SELECT COUNT(*) FROM peers);
BEGIN
    RETURN query
    WITH success AS (
       SELECT COUNT(peer)
       FROM checks
       JOIN p2p ON checks.id = p2p.check
       JOIN peers ON checks.peer = peers.nickname
       JOIN verter ON checks.id = verter.check
       WHERE date_part('day', checks.data) = date_part('day', peers.birthday)
		AND date_part('month', checks.data) = date_part('month', peers.birthday) AND verter.state = 'success' AND p2p.state = 'success'
    ),
    fail AS (
        SELECT COUNT(peer)
       FROM checks
       JOIN p2p ON checks.id = p2p.check
       JOIN peers ON checks.peer = peers.nickname
       JOIN verter ON checks.id = verter.check
       WHERE date_part('day', checks.data) = date_part('day', peers.birthday)
		AND date_part('month', checks.data) = date_part('month', peers.birthday) AND verter.state = 'failure' AND p2p.state = 'failure'
    )
    SELECT success.count * 100 / cnt, fail.count * 100 / cnt
    FROM success
    CROSS JOIN fail;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_getPercent();

-- 11) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3

CREATE OR REPLACE FUNCTION fnc_checktask(first_task VARCHAR, second_task VARCHAR, third_task VARCHAR)
RETURNS TABLE (
    Peer VARCHAR
) AS
$$
BEGIN
    RETURN query
    WITH one AS (
        SELECT DISTINCT checks.peer
        FROM checks
        JOIN verter ON checks.id = verter.check
        JOIN p2p ON checks.id = p2p.check
        WHERE task = first_task AND p2p.state = 'success' AND verter.state = 'success'
    ),
    two AS (
        SELECT DISTINCT checks.peer
        FROM checks
        JOIN verter ON checks.id = verter.check
        JOIN p2p ON checks.id = p2p.check
        WHERE task = second_task AND p2p.state = 'success' AND verter.state = 'success'
    ),
    three AS (
        SELECT DISTINCT checks.peer
        FROM checks
        JOIN verter ON checks.id = verter.check
        JOIN p2p ON checks.id = p2p.check
        WHERE task = third_task AND p2p.state = 'success' AND verter.state = 'success'
    )
    SELECT nickname
    FROM peers
    WHERE nickname IN (SELECT one.peer FROM one) AND nickname IN (SELECT two.peer FROM two) AND
    nickname NOT IN (SELECT three.peer FROM three) ;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_checktask('C1', 'C2', 'C3');

-- 12) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач

CREATE OR REPLACE FUNCTION fnc_getPrevCount()
RETURNS TABLE (
    Task VARCHAR,
    PrevCount INT
) AS
$$
BEGIN
    RETURN query
    WITH RECURSIVE t AS (
        SELECT title AS Task, 0 AS PrevCount
        FROM tasks
        WHERE parent_task IS NULL
        UNION ALL
        SELECT title, t.PrevCount + 1
        FROM tasks
        JOIN t ON parent_task = t.Task
    )
    SELECT t.Task, t.PrevCount
    FROM t;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_getPrevCount();

-- 13) Найти "удачные" для проверок дни

CREATE OR REPLACE FUNCTION fnc_getGoodDays(n INT)
RETURNS TABLE (
    Day DATE
) AS
$$
BEGIN
    RETURN query
    WITH success_checks AS (
        SELECT checks.id, checks.task, checks.data, p2p.time, p2p.state, xp.xp_amount
        FROM checks
        JOIN p2p ON checks.id = p2p.id
        JOIN xp ON checks.id = xp.id
        WHERE (state = 'success' OR state = 'failure')
        ORDER BY checks.data, p2p.time
    ),
    success_dates AS (
        SELECT id, data, time, state,
            (CASE WHEN (state = 'success' AND xp_amount >= max_xp * 0.8)
            THEN ROW_NUMBER() over (partition by state, data) ELSE 0 END) AS amount
        FROM success_checks
        JOIN tasks ON title = task
        ORDER BY data
    ),
    max_dates AS (
        SELECT data, MAX(amount) AS amount
        FROM success_dates
        GROUP BY data
    )
    SELECT data
    FROM max_dates
    WHERE amount >= n;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM fnc_getGoodDays(1);

-- 14) Определить пира с наибольшим количеством XP

CREATE OR REPLACE FUNCTION fnc_getPeerMaxXp()
RETURNS TABLE (
    Peer VARCHAR,
    Xp BIGINT
) AS
$$
BEGIN
    RETURN query
    SELECT checks.peer, SUM(xp_amount) AS xp
    FROM xp
    JOIN checks ON xp.check = checks.id
    GROUP BY checks.peer
    ORDER BY xp DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_getPeerMaxXp();

-- 15) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время

CREATE OR REPLACE FUNCTION fnc_getPeerCome(t TIME, n INT)
RETURNS TABLE (
    Peer VARCHAR
    ) AS
$$
BEGIN
	RETURN QUERY
    SELECT p.peer
    FROM (SELECT tt.peer
		  FROM time_tracking AS tt
		  GROUP BY tt.peer, data
		  HAVING MIN(time) < t ) AS p
	GROUP BY p.peer
    HAVING COUNT(p.peer) >= n;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_getPeerCome(TIME '23:00:00', 1);

-- 16) Определить пиров, выходивших за последние N дней из кампуса больше M раз

CREATE OR REPLACE FUNCTION fnc_getPeerExit(n INT, m INT)
RETURNS TABLE (
    Peer VARCHAR
    ) AS
$$
BEGIN
	RETURN QUERY
	WITH t AS (
	SELECT p.peer, data, count
    FROM (SELECT tt.peer, data, COUNT(state) - 1 AS count
		  FROM time_tracking AS tt
	   	  WHERE state = 2
		  GROUP BY tt.peer, data) AS p
	WHERE (CURRENT_DATE - data) < n
    )
	SELECT t.peer FROM t
	GROUP BY t.peer, count
	HAVING count >= m;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_getPeerExit(100, 0);

-- 17) Определить для каждого месяца процент ранних входов

CREATE OR REPLACE FUNCTION fnc_getPeerEntry()
RETURNS TABLE(
    Month TEXT,
    EarlyEntries NUMERIC
) AS
$$
BEGIN
	RETURN QUERY
	SELECT p.Month,
    ROUND((COUNT(Entries) FILTER (WHERE Entries < TIME '12:00:00'))::DECIMAL / COUNT(Entries) * 100) AS EarlyEntries
	FROM (SELECT TO_CHAR(birthday, 'Month') AS Month, MIN(time) AS Entries
	      FROM time_tracking
		  JOIN peers ON peer = nickname
	      GROUP BY TO_CHAR(Birthday, 'Month'), peer, data) AS p
	GROUP BY p.Month
	ORDER BY TO_DATE(p.Month, 'Month');
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fnc_getPeerEntry();
