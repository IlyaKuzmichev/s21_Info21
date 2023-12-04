---------------------------- 2.1 ----------------------------------

CREATE OR REPLACE PROCEDURE add_p2p_check(checked_peer varchar, checking_peer varchar,
                                        task varchar, status check_status, "time" time)
AS $$
    DECLARE
        check_id integer;
    BEGIN
        IF status = 'Start' THEN
            INSERT INTO checks(peer, task, "date")
                   VALUES (checked_peer, task, CURRENT_DATE)
         RETURNING id
              INTO check_id;
        ELSE
            SELECT "check"
              FROM (
                   SELECT "check",
                          bool_and(p2p.state NOT IN ('Success', 'Failure')) OVER check_window AS unfinished
                     FROM p2p
                          INNER JOIN checks c on p2p."check" = c.id
                    WHERE p2p.checking_peer = add_p2p_check.checking_peer
                          AND c.peer = checked_peer
                          AND c.task = add_p2p_check.task
                          AND p2p.time <= add_p2p_check."time"
                   WINDOW check_window AS (PARTITION BY c.id)
            ) same_checks
            WHERE unfinished
            LIMIT 1
            INTO check_id;
        END IF;

        INSERT INTO p2p("check", checking_peer, state, time)
               VALUES (check_id, add_p2p_check.checking_peer, add_p2p_check.status, add_p2p_check."time");
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM p2p;

CALL add_p2p_check('Baboba', 'Derevo', 'DO3_LinuxMonitoring_v1.0', 'Start', '13:00');
CALL add_p2p_check('Baboba', 'Derevo', 'DO3_LinuxMonitoring_v1.0', 'Success', '13:15');

CALL add_p2p_check('Aboba', 'Baboba', 'C4_s21_math', 'Start', '14:00:05');
CALL add_p2p_check('Aboba', 'Baboba', 'C4_s21_math', 'Failure', '14:23:55');

SELECT * FROM p2p;

---------------------------- 2.2 ----------------------------------

CREATE OR REPLACE PROCEDURE add_verter_check(checked_peer varchar, task varchar,
                                            status check_status, "time" time)
AS $$
    DECLARE
        check_id integer;
    BEGIN
        SELECT id
          FROM (
               SELECT c.id, p2p."time" p2p_time, MAX(p2p."time") OVER (PARTITION BY c.peer, c.task) AS max_p2p_time
                 FROM p2p
                      INNER JOIN checks c on c.id = p2p."check"
                WHERE c.peer = checked_peer
                      AND c.task = add_verter_check.task
                      AND p2p.state = 'Success'
          ) same_checks
         WHERE same_checks.p2p_time = same_checks.max_p2p_time
         LIMIT 1
          INTO check_id;

        INSERT INTO verter("check", state, "time")
               VALUES (check_id, status, add_verter_check."time");
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM verter;

CALL add_verter_check('Baboba', 'DO3_LinuxMonitoring_v1.0', 'Start', '13:15');
CALL add_verter_check('Baboba', 'DO3_LinuxMonitoring_v1.0', 'Success', '13:16');

SELECT * FROM verter;

---------------------------- 2.3 ----------------------------------

CREATE OR REPLACE FUNCTION fnc_trg_transfer_p2p_point() RETURNS trigger
AS $$
    DECLARE
        new_checked_peer varchar;
        transferred_points_id integer;
    BEGIN
       SELECT peer
         FROM checks
        WHERE id = NEW."check"
         INTO new_checked_peer;

       SELECT id
         FROM transferred_points tp
        WHERE checking_peer = NEW.checking_peer
              AND checked_peer = new_checked_peer
         INTO transferred_points_id;

       IF transferred_points_id IS NULL THEN
           INSERT INTO transferred_points(checking_peer, checked_peer, points_amount)
                  VALUES (NEW.checking_peer, new_checked_peer, 1);
       ELSE
           UPDATE transferred_points
              SET points_amount = points_amount + 1
            WHERE checking_peer = NEW.checking_peer
                  AND checked_peer = new_checked_peer;
       END IF;

       RETURN NULL;
    END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_transfer_p2p_point ON p2p;

CREATE TRIGGER trg_transfer_p2p_point
AFTER INSERT
ON p2p
FOR EACH ROW WHEN (NEW.state = 'Start')
EXECUTE FUNCTION fnc_trg_transfer_p2p_point();

SELECT * FROM transferred_points;

CALL add_p2p_check('Baboba', 'Egogo', 'DO5_SimpleDocker', 'Start', '18:00');

SELECT * FROM transferred_points;

CALL add_p2p_check('Baboba', 'Egogo', 'DO5_SimpleDocker', 'Success', '18:15');
CALL add_p2p_check('Baboba', 'Egogo', 'DO6_CICD', 'Start', '18:30');

SELECT * FROM transferred_points;

---------------------------- 2.4 ----------------------------------

CREATE OR REPLACE FUNCTION fnc_trg_xp_validation() RETURNS trigger
AS $$
    DECLARE
        task_max_xp integer;
    BEGIN
        SELECT t.max_xp
          FROM (SELECT NEW.*) new_xp
               INNER JOIN checks c ON new_xp."check" = c.id
               INNER JOIN tasks t ON c.task = t.title
         LIMIT 1
          INTO task_max_xp;

        IF NEW.xp_amount > task_max_xp THEN
            RAISE EXCEPTION 'inserted xp amount % is greater than max xp % for task',
                NEW.xp_amount, task_max_xp;
        END IF;

        IF EXISTS (
            SELECT COUNT(*)
              FROM checks
                   LEFT JOIN p2p p on checks.id = p."check"
                   LEFT JOIN verter v on checks.id = v."check"
             WHERE checks.id = NEW."check"
             GROUP BY checks.id
            HAVING NOT bool_or(p.state = 'Success')
                   OR (bool_or(v.state IS NOT NULL)
                       AND NOT bool_or(v.state = 'Success'))
        ) THEN
            RAISE EXCEPTION 'XP can be gained only for successful check';
        END IF;

        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_xp_validation ON xp;

CREATE TRIGGER trg_xp_validation
BEFORE INSERT
ON xp
FOR EACH ROW EXECUTE FUNCTION fnc_trg_xp_validation();

SELECT * FROM xp;

CALL add_p2p_check('Baboba', 'Egogo', 'DO6_CICD', 'Success', '18:35');

INSERT INTO xp("check", xp_amount) 
       VALUES ((SELECT id FROM checks WHERE peer = 'Baboba' AND task = 'DO6_CICD' LIMIT 1), 1050);

SELECT * FROM xp;

INSERT INTO xp("check", xp_amount)
       VALUES ((SELECT id FROM checks WHERE peer = 'Baboba' AND task = 'DO6_CICD' LIMIT 1), 400);

SELECT * FROM xp;

CALL add_p2p_check('Baboba', 'Egogo', 'DO4_LinuxMonitoring_v2.0', 'Start', '20:15');

CALL add_p2p_check('Baboba', 'Egogo', 'DO4_LinuxMonitoring_v2.0', 'Failure', '20:20');

INSERT INTO xp("check", xp_amount)
       VALUES ((SELECT id FROM checks WHERE peer = 'Baboba' AND task = 'DO4_LinuxMonitoring_v2.0' LIMIT 1), 450);

SELECT * FROM xp;
