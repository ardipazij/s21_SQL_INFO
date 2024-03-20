--Для данной части задания вам нужно создать отдельную базу данных
CREATE DATABASE part_4;
-- schema for database part_4
DROP TABLE TableName1;
CREATE TABLE IF NOT EXISTS TableName1
(
    id    SERIAL PRIMARY KEY,
    name  VARCHAR(50),
    value INT
);

CREATE TABLE IF NOT EXISTS TableName2
(
    id          SERIAL PRIMARY KEY,
    description TEXT,
    status      BOOLEAN
);

CREATE TABLE IF NOT EXISTS OtherTable1
(
    id    SERIAL PRIMARY KEY,
    data1 VARCHAR(50),
    data2 INT
);

CREATE TABLE IF NOT EXISTS OtherTable2
(
    id   SERIAL PRIMARY KEY,
    info TEXT
);

-- insert data to tables
INSERT INTO TableName1 (name, value)
VALUES ('Danil', 10),
       ('Kate', 20),
       ('Timur', 15);

INSERT INTO TableName2 (description, status)
VALUES ('Status part_1', FALSE),
       ('Status part_2', TRUE),
       ('Status part_3', FALSE);

INSERT INTO OtherTable1 (data1, data2)
VALUES ('Milk', 100),
       ('Bread', 200),
       ('Butter', 150);

INSERT INTO OtherTable2 (info)
VALUES ('Opana moment'),
       ('Ak Bars'),
       ('Dinamo');

CREATE OR REPLACE FUNCTION scalar_function(input_value INT)
    RETURNS INT AS
$$
DECLARE
    result INT;
BEGIN
    result := input_value * input_value;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION non_scalar_function()
    RETURNS TABLE
            (
                id                INT,
                concatenated_data TEXT
            )
AS
$$
BEGIN
    RETURN QUERY SELECT id, data1 || ' - ' || data2 FROM OtherTable1;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION before_insert_trigger_function()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.status := TRUE;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER before_insert_trigger
    BEFORE INSERT
    ON TableName2
    FOR EACH ROW
EXECUTE FUNCTION before_insert_trigger_function();

CREATE OR REPLACE FUNCTION after_insert_trigger_function()
    RETURNS TRIGGER AS
$$
DECLARE
    new_id INT;
BEGIN
    SELECT id INTO new_id FROM TableName1 WHERE name = NEW.name;

    INSERT INTO OtherTable2 (id, info) VALUES (new_id, 'Inserted');

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_insert_trigger
    AFTER INSERT
    ON TableName1
    FOR EACH ROW
EXECUTE FUNCTION after_insert_trigger_function();

CREATE OR REPLACE FUNCTION non_dml_trigger_function()
    RETURNS event_trigger AS
$$
BEGIN
    RAISE NOTICE 'ITS NOT DML, ITS DDL, AHAHAHAHAHAHAHA';
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER my_non_dml_event_trigger
    ON ddl_command_end
EXECUTE FUNCTION non_dml_trigger_function();

SELECT *
from information_schema.tables

-- Создание и заполнение этой базы данных, а также написанные процедуры, внести в файл part4.sql.

-- 1) Создать хранимую процедуру, которая, не уничтожая базу данных, уничтожает все те таблицы текущей базы данных, имена которых начинаются с фразы 'TableName'.

CREATE OR REPLACE PROCEDURE delete_tablename_mask_tables()
    language plpgsql
as
$$
DECLARE
    required_table_name text;
begin
    FOR required_table_name IN (SELECT table_name
                                FROM information_schema.tables
                                WHERE table_name LIKE 'tablename' || '%')
        LOOP
            EXECUTE 'DROP TABLE IF EXISTS ' || required_table_name || ' CASCADE';
        END LOOP;
end;
$$;

CALL delete_tablename_mask_tables();

-- 2) Создать хранимую процедуру с выходным параметром, которая выводит список имен и параметров всех скалярных SQL функций пользователя в текущей базе данных. Имена функций без параметров не выводить. Имена и список параметров должны выводиться в одну строку. Выходной параметр возвращает количество найденных функций.
CREATE OR REPLACE PROCEDURE part_two()
    LANGUAGE plpgsql AS
$$
BEGIN
    DECLARE
        rec RECORD;
    BEGIN
        FOR rec IN (SELECT proname                                AS function_name,
                           pg_get_function_arguments(pg_proc.oid) AS function_arguments,
                           pg_get_function_result(pg_proc.oid)    AS function_result
                    FROM pg_proc
                             JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
                    WHERE pg_namespace.nspname = 'public'
                      AND pg_proc.prokind = 'f'
                      AND pg_proc.pronargs >= 0
                      AND pg_get_function_arguments(pg_proc.oid) != '')
            LOOP
                RAISE NOTICE 'function_name: % ; function_arguments: % ; function_result: %', rec.function_name, rec.function_arguments, rec.function_result;
            END LOOP;
    END;
END;
$$;

CALL part_two();

DROP PROCEDURE part_two();

-- 3) Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL DML триггеры в текущей базе данных. Выходной параметр возвращает количество уничтоженных триггеров.
SELECT *
FROM information_schema.triggers;
SELECT *
FROM pg_event_trigger;

CREATE OR REPLACE PROCEDURE part_3(OUT triggers_count INT)
    LANGUAGE plpgsql
AS
$$
DECLARE
    req RECORD;
BEGIN
    triggers_count := 0;
    FOR req IN (SELECT trigger_name, event_object_schema AS trigger_schema, event_object_table AS trigger_table
                FROM information_schema.triggers)
        LOOP
            EXECUTE 'DROP TRIGGER IF EXISTS ' || req.trigger_name || ' ON ' || req.trigger_schema || '.' ||
                    req.trigger_table || ' CASCADE';
            triggers_count := triggers_count + 1;
        END LOOP;
END;
$$;
DO
$$
    DECLARE
        triggers_count INT DEFAULT 0;
    BEGIN
        CALL part_3(triggers_count);
        RAISE NOTICE 'Total destroyed triggers: %', triggers_count;
    END
$$;

SELECT *
FROM information_schema.triggers;
SELECT *
FROM pg_event_trigger;
--
--
--
-- -- Создать хранимую процедуру с входным параметром, которая выводит имена и описания типа объектов (только хранимых процедур и скалярных функций), в тексте которых на языке SQL встречается строка, задаваемая параметром процедуры.
CREATE OR REPLACE PROCEDURE part_4(IN count_objects INT DEFAULT 1)
    LANGUAGE plpgsql
AS
$$
DECLARE
    req           RECORD;
    objects_count INT := 0;
BEGIN
    FOR req IN (SELECT proname AS obj_name,
                       CASE
                           WHEN prokind = 'p' THEN 'procedure'
                           WHEN prokind = 'f' THEN 'FUNC'
                           ELSE 'unknown'
                           END AS object_type
                FROM pg_catalog.pg_proc
                         JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
                WHERE pg_namespace.nspname = 'public'
                  AND (pg_proc.prokind = 'f' OR pg_proc.prokind = 'p')
                  AND pg_proc.pronargs >= 0)
        LOOP
            RAISE NOTICE 'obj_name: % ; object_type: % ', req.obj_name, req.object_type;
            objects_count := objects_count + 1;
            IF objects_count = count_objects THEN
                EXIT;
            END IF;
        END LOOP;
END;
$$;

CALL part_4();

CALL part_4(5);