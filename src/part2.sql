-- 1) Написать процедуру добавления P2P-проверки
-- Параметры: ник проверяемого, ник проверяющего, название задачи, статус P2P проверки , время.
-- Если статус «Начало», добавьте запись в таблицу «Проверки» (используйте сегодняшнюю дату).
-- Добавьте запись в таблицу P2P.
-- Если статус «начало», в качестве проверки укажите только
-- что добавленную запись, в противном случае укажите проверку с незавершенным P2P-шагом.

CREATE OR REPLACE PROCEDURE pr_p2p (peers_pro VARCHAR(30), checking_peer_pro VARCHAR(30), task_pro VARCHAR(60), "state" VARCHAR(30), "time" TIME)
language plpgsql AS
$$
    BEGIN
    CASE
        WHEN ("state" = 'start') THEN
        INSERT INTO checks (id, peer, task, data)
        VALUES ((SELECT MAX(id) + 1 FROM checks), peers_pro, task_pro, CURRENT_DATE);
        INSERT INTO p2p (id, "check", checking_peer, state, time)
        VALUES ((SELECT MAX(id) + 1 FROM p2p), (SELECT MAX(id) FROM checks), checking_peer_pro, "state", "time");
    ELSE
        INSERT INTO p2p (id, "check", checking_peer, state, time)
        VALUES ((SELECT MAX(id) + 1 FROM p2p), (SELECT id FROM (SELECT checks.id AS id, peer, task FROM checks INNER JOIN p2p ON checks.id = p2p."check" GROUP BY checks.id HAVING COUNT(*) = 1) AS select_1 WHERE peers_pro = peer AND task_pro = task), checking_peer_pro, "state", "time");
    END CASE;
    END;
$$;
-- Проверка
-- 1. Если статус "Начало", то добавляем запись в checks and p2p
CALL pr_p2p ('latorias', 'loquatsr', 'C1', 'start', '22:12');
-- 2. Если статус отдично от "Начало", то добавляем запись в checks and p2p
-- 2.1 Если статус "Success"
CALL pr_p2p ('latorias', 'loquatsr', 'C1', 'success', '22:48');
-- 2.2 Если статус "Failure"
CALL pr_p2p ('latorias', 'loquatsr', 'C1', 'failure', '22:48');




-- 2) Написать процедуру добавления проверки Verterом
-- Параметры: ник проверяемого, название задания, статус проверки Verterом, время.
-- Добавить запись в таблицу Verter (в качестве проверки указать проверку соответствующего задания с самым поздним (по времени) успешным P2P этапом)

CREATE OR REPLACE PROCEDURE verter (peers_pro VARCHAR(30), task_pro VARCHAR(60), "state_pro" VARCHAR(30), "time" TIME)
language plpgsql AS
$$
    BEGIN
        INSERT INTO verter (id, "check", state, time)
        VALUES ((SELECT MAX(id) + 1 FROM verter), (SELECT id FROM (SELECT id, data FROM (SELECT checks.id AS id, peer, task, p2p.state AS state, data FROM checks INNER JOIN p2p ON checks.id = p2p."check") AS select_2
        WHERE state = 'success' AND peers_pro = peer AND task_pro = task)
            AS select_3 ORDER BY data LIMIT 1),
               state_pro, "time");
    END;
$$;
-- Проверка
CALL verter ('latorias', 'C1', 'start', '22:35');
CALL verter ('latorias', 'C1', 'success', '22:46');
CALL verter ('latorias', 'C1', 'failure', '22:46');
-- 3) Написать триггер: после добавления записи со статутом "начало" в таблицу P2P, изменить соответствующую запись в таблице TransferredPoints

CREATE OR REPLACE FUNCTION fn_TransferredPoints() RETURNS TRIGGER
AS $tg_TransferredPoints$
    BEGIN
        IF (TG_OP = 'INSERT' AND NEW.state = 'start') THEN
            INSERT INTO transferred_points SELECT (SELECT MAX(id) + 1 FROM transferred_points), (SELECT peer FROM (SELECT checks.id AS id, peer FROM checks INNER JOIN p2p ON checks.id = p2p."check" ORDER BY id DESC LIMIT 1) AS select_4), NEW.checking_peer, '1';
        END IF;
        RETURN NULL;
 END;
$tg_TransferredPoints$ language plpgsql;

CREATE TRIGGER transferred_points
AFTER INSERT ON p2p
    FOR EACH ROW EXECUTE FUNCTION fn_TransferredPoints();

-- 4) Написать триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи
-- Запись считается корректной, если:
--
-- Количество XP не превышает максимальное доступное для проверяемой задачи
-- Поле Check ссылается на успешную проверку
-- Если запись не прошла проверку, не добавлять её в таблицу.

CREATE OR REPLACE FUNCTION fn_Xp() RETURNS TRIGGER
AS $tg_Xp$
    BEGIN
        IF (TG_OP = 'INSERT' AND NEW.xp_amount <= (SELECT tasks.max_xp FROM checks INNER JOIN tasks ON checks.task = tasks.title WHERE checks.id = NEW.check)
                AND (SELECT state FROM verter WHERE verter.check = NEW.check AND verter.state = 'success') = 'success')
        THEN
        ELSE
        RAISE EXCEPTION 'Количество начисляемого опыта превышает допустимый лимит';
         END IF;
    RETURN NEW;
 END;
$tg_Xp$ language plpgsql;

CREATE TRIGGER xp
AFTER INSERT ON xp
    FOR EACH ROW EXECUTE FUNCTION fn_Xp();

INSERT INTO xp (id, "check", xp_amount)
VALUES ((SELECT MAX(id) + 1 FROM xp), '11', '250');

drop function fn_xp() cascade;



