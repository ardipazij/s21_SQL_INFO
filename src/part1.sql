drop table p2p cascade;
drop table verter cascade;
drop table transferred_points cascade;
drop table friends cascade;
drop table recommendations cascade;
drop table time_tracking cascade;
drop table xp cascade;
drop table checks cascade;
drop table peers cascade;
drop table tasks cascade;

CREATE TABLE IF NOT EXISTS peers
(
    nickname VARCHAR(30) PRIMARY KEY NOT NULL,
    birthday DATE
);

CREATE TABLE IF NOT EXISTS tasks
(
    title VARCHAR(60) PRIMARY KEY,
    parent_task VARCHAR(60) REFERENCES  tasks (title),
    max_xp INT
);

CREATE TABLE IF NOT EXISTS checks
(
    id INT PRIMARY KEY NOT NULL,
    peer VARCHAR(30) REFERENCES peers (nickname) NOT NULL,
    task VARCHAR(30) REFERENCES tasks (title) NOT NULL,
    data DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS p2p
(
    id INT PRIMARY KEY,
    "check" INT REFERENCES checks (id) NOT NULL,
    checking_peer VARCHAR(30) REFERENCES peers (nickname),
    state VARCHAR(30) DEFAULT 'failure',
    time TIME NOT NULL
);

CREATE TABLE IF NOT EXISTS verter
(
    id INT PRIMARY KEY NOT NULL,
    "check" INT REFERENCES checks (id) NOT NULL,
    state VARCHAR(30) NOT NULL,
    time TIME NOT NULL
);

CREATE TABLE IF NOT EXISTS checks
(
    id INT PRIMARY KEY NOT NULL,
    peer VARCHAR(30) REFERENCES peers (nickname) NOT NULL,
    task VARCHAR(60) NOT NULL,
    date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS transferred_points
(
    id INT PRIMARY KEY NOT NULL,
    checking_peer VARCHAR(30) REFERENCES peers (nickname) NOT NULL,
    checked_peer VARCHAR(30) REFERENCES peers (nickname) NOT NULL,
    points_amount INT DEFAULT 1
);

CREATE TABLE IF NOT EXISTS friends
(
    id INT PRIMARY KEY NOT NULL,
    peer_1 VARCHAR(30) REFERENCES peers (nickname) NOT NULL,
    peer_2 VARCHAR(30) REFERENCES peers (nickname) NOT NULL
);

CREATE TABLE IF NOT EXISTS time_tracking
(
    id INT PRIMARY KEY NOT NULL,
    peer VARCHAR(30) REFERENCES peers (nickname) NOT NULL,
    data DATE NOT NULL,
    time TIME NOT NULL,
    state INT CHECK (state = 1 OR state = 2)
);
CREATE TABLE IF NOT EXISTS recommendations
(
    id INT PRIMARY KEY NOT NULL,
    peer VARCHAR(30) REFERENCES peers (nickname) NOT NULL,
    recommended_peer VARCHAR(30) REFERENCES peers (nickname)NOT NULL
);

CREATE TABLE IF NOT EXISTS xp
(
    id INT PRIMARY KEY NOT NULL,
    "check" INT REFERENCES checks (id) NOT NULL,
    xp_amount INT NOT NULL
);

CREATE OR REPLACE PROCEDURE moving (type text, "table" text,  delimiter text)
language plpgsql AS
$$
    DECLARE path text := '/Users/loquatsr/SQL2_Info21_v1.0-1/src/';
--         DECLARE path text := '/home/danil/SQL2_Info21_v1.0-1/src/';
    BEGIN
    CASE
        WHEN (type = 'import') THEN EXECUTE 'COPY ' || "table" || ' FROM ' || '''' || path || 'import/'
        || "table" || '.csv' || '''' || ' DELIMITER ' || '''' || delimiter || ''''  || ' CSV HEADER;';
        ELSE EXECUTE 'COPY ' || "table" || ' TO' || '''' || path || 'export/' || "table" || '.csv' || '''' || ' CSV HEADER;';
    END CASE;
    END;
$$;

CALL moving ('import', 'peers', ',');
CALL moving ('import', 'time_tracking', ',');
CALL moving ('import', 'transferred_points', ',');
CALL moving ('import', 'friends', ',');
CALL moving ('import', 'recommendations', ',');
CALL moving ('import', 'tasks', ',');
CALL moving ('import', 'checks', ',');
CALL moving ('import', 'p2p', ',');
CALL moving ('import', 'verter', ',');
CALL moving ('import', 'xp', ',');

CALL moving ('export', 'checks', ',');
CALL moving ('export', 'friends', ',');
CALL moving ('export', 'p2p', ',');
CALL moving ('export', 'peers', ',');
CALL moving ('export', 'recommendations', ',');
CALL moving ('export', 'tasks', ',');
CALL moving ('export', 'time_tracking', ',');
CALL moving ('export', 'transferred_points', ',');
CALL moving ('export', 'verter', ',');
CALL moving ('export', 'xp', ',');





