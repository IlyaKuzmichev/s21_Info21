---------------------------- 3.1 ----------------------------------
-- Write a function that returns the TransferredPoints table in a more human-readable form
-- Peer's nickname 1, Peer's nickname 2, number of transferred peer points.
-- The number is negative if peer 2 received more points from peer 1.

DROP FUNCTION IF EXISTS get_transferred_points_summary();
CREATE OR REPLACE FUNCTION get_transferred_points_summary()
RETURNS TABLE (peer_1 VARCHAR, peer_2 VARCHAR,points_amount BIGINT)
AS $$
BEGIN
    RETURN QUERY
      WITH paired_points AS (
    SELECT tp1.checking_peer AS peer1_name,
           tp1.checked_peer AS peer2_name,
           SUM(tp1.points_amount - COALESCE(tp2.points_amount, 0)) AS points
      FROM transferred_points AS tp1
           LEFT JOIN transferred_points AS tp2
           ON tp1.checked_peer = tp2.checking_peer
           AND tp1.checking_peer = tp2.checked_peer
     GROUP BY tp1.checking_peer, tp1.checked_peer
    )
SELECT ranked_points.peer1_name,
       ranked_points.peer2_name,
       ranked_points.points
  FROM (
       SELECT peer1_name,
              peer2_name,
              points, --need to think how to choose first unique pair peer_1 peer_2 more easy...
              ROW_NUMBER() OVER(PARTITION BY
                  LEAST(peer1_name, peer2_name),
                  GREATEST(peer1_name, peer2_name)
                  ORDER BY peer1_name, peer2_name) AS rn
         FROM paired_points
  ) ranked_points
 WHERE rn = 1;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM transferred_points;

SELECT * FROM get_transferred_points_summary();

INSERT INTO transferred_points(checking_peer, checked_peer, points_amount)
VALUES ('Aboba', 'Baboba', 5);

INSERT INTO transferred_points(checking_peer, checked_peer, points_amount)
VALUES ('Baboba', 'Cagoga', 3);

SELECT * FROM get_transferred_points_summary();

---------------------------- 3.2 ----------------------------------
-- Write a function that returns a table of the following form: user name, name of the checked task, number of XP received
-- Include in the table only tasks that have successfully passed the check (according to the Checks table).
-- One task can be completed successfully several times. In this case, include all successful checks in the table.

DROP FUNCTION IF EXISTS  get_peer_xp_info();
CREATE OR REPLACE FUNCTION get_peer_xp_info()
RETURNS TABLE (peer VARCHAR, task TEXT, xp INTEGER)
AS $$
BEGIN
    RETURN QUERY
    SELECT p.nickname,
           SPLIT_PART(c.task, '_', 1),
           xp.xp_amount
      FROM checks AS c
           INNER JOIN peers AS p
           ON c.peer = p.nickname
           INNER JOIN p2p
           ON c.id = p2p."check"
           AND p2p.state = 'Success'
           INNER JOIN xp ON
           c.id = xp."check"
           LEFT JOIN verter AS v
           ON c.id = v."check"
     WHERE v.state = 'Success'
           OR v.state IS NULL;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_peer_xp_info();

---------------------------- 3.3 ----------------------------------
-- Write a function that finds the peers who have not left campus for the whole day
-- Function parameters: day, for example 12.05.2022.
-- The function returns only a list of peers.

DROP FUNCTION IF EXISTS get_whole_day_peers(day DATE);
CREATE OR REPLACE FUNCTION get_whole_day_peers(day DATE)
RETURNS TABLE (peer VARCHAR)
AS $$
BEGIN
    RETURN QUERY
    SELECT p.nickname
      FROM peers AS p
     WHERE (
           SELECT COUNT(*)
             FROM time_tracking AS tt
            WHERE tt.peer = p.nickname
              AND tt."date" = day
              AND tt.state = 2) = 1;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM time_tracking;

SELECT * FROM get_whole_day_peers('2023-09-24');

INSERT INTO time_tracking(peer, date, time, state)
VALUES ('Aboba', '2023-09-24', '20:05:00', 1);

INSERT INTO time_tracking(peer, date, time, state)
VALUES ('Aboba', '2023-09-24', '21:21:21', 2);

SELECT * FROM get_whole_day_peers('2023-09-24');

SELECT * FROM get_whole_day_peers('2023-09-25');

---------------------------- 3.4 ----------------------------------
-- Calculate the change in the number of peer points of each peer using the TransferredPoints table
-- Output the result sorted by the change in the number of points.
-- Output format: peer's nickname, change in the number of peer points

DROP FUNCTION IF EXISTS calculate_peer_points_change();
CREATE OR REPLACE FUNCTION calculate_peer_points_change()
RETURNS TABLE (peer VARCHAR, points_change NUMERIC)
AS $$
BEGIN
    RETURN QUERY
      WITH transfered AS (
    SELECT checking_peer AS peer_name,
           SUM(points_amount) AS points_change
      FROM transferred_points
     GROUP BY checking_peer
     UNION ALL
    SELECT checked_peer AS peer_name,
           -SUM(points_amount) AS points_change
      FROM transferred_points
     GROUP BY checked_peer
    )
    SELECT peer_name,
           SUM(t.points_change)
      FROM transfered AS t
     GROUP BY peer_name
     ORDER BY SUM(t.points_change) DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM transferred_points;

SELECT * FROM calculate_peer_points_change();

---------------------------- 3.5 ----------------------------------
-- Calculate the change in the number of peer points of each peer using the table returned by the first function from Part 3
-- Output the result sorted by the change in the number of points.
-- Output format: peer's nickname, change in the number of peer points

DROP FUNCTION IF EXISTS fast_calculate_peer_points_change();
CREATE OR REPLACE FUNCTION fast_calculate_peer_points_change()
RETURNS TABLE (peer VARCHAR, points_change NUMERIC)
AS $$
BEGIN
    RETURN QUERY
      WITH transfered AS (
    SELECT peer_1 AS peer_name,
           SUM(points_amount) AS points_change
      FROM get_transferred_points_summary()
     GROUP BY peer_1
     UNION ALL
    SELECT peer_2 AS peer_name,
           -SUM(points_amount) AS points_change
      FROM get_transferred_points_summary()
     GROUP BY peer_2
    )
    SELECT peer_name,
           SUM(t.points_change)
      FROM transfered AS t
     GROUP BY peer_name
     ORDER BY SUM(t.points_change) DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM fast_calculate_peer_points_change();

---------------------------- 3.6 ----------------------------------
-- Find the most frequently checked task for each day
-- If there is the same number of checks for some tasks in a certain day, output all of them.
-- Output format: day, task name

DROP FUNCTION IF EXISTS get_popular_tasks_for_check();
CREATE OR REPLACE FUNCTION get_popular_tasks_for_check()
RETURNS TABLE (day DATE, task TEXT)
AS $$
BEGIN
    RETURN QUERY
      WITH daily_check_counts AS (
    SELECT "date",
           checks.task AS popular_task,
           COUNT(*) AS check_count
      FROM checks
     GROUP BY "date",
              checks.task
    ),
           ranked_daily_check_counts AS (
    SELECT "date",
           popular_task,
           check_count,
           DENSE_RANK() OVER (PARTITION BY
               "date"
               ORDER BY check_count DESC) AS rank
      FROM daily_check_counts
    )
    SELECT rd."date",
           SPLIT_PART(rd.popular_task, '_', 1)
      FROM ranked_daily_check_counts AS rd
     WHERE rank = 1
     ORDER BY "date";
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_popular_tasks_for_check();

---------------------------- 3.7 ----------------------------------
-- Find all peers who have completed the whole given block of tasks and the completion date of the last task
-- Procedure parameters: name of the block, for example “CPP”.
-- The result is sorted by the date of completion.
-- Output format: peer's name, date of completion of the block (i.e. the last completed task from that block)

DROP FUNCTION IF EXISTS get_peers_completed_whole_block(block VARCHAR);
CREATE OR REPLACE FUNCTION get_peers_completed_whole_block(block VARCHAR)
RETURNS TABLE (peer VARCHAR, day DATE)
AS $$
BEGIN
    RETURN QUERY
      WITH tasks_in_block AS (
    SELECT title
      FROM tasks
     WHERE title ~ CONCAT(block, '[0-9].*')
    )
    SELECT c.peer,
           MAX(c.date) AS last_date
      FROM checks AS c
           INNER JOIN tasks_in_block AS tb
           ON c.task = tb.title
           INNER JOIN xp
           ON xp."check" = c.id
     GROUP BY c.peer
    HAVING COUNT(DISTINCT c.task) = (SELECT COUNT(*) FROM tasks_in_block);
END;
$$ LANGUAGE plpgsql;

CALL add_p2p_check('Aboba', 'Cagoga', 'A1_Maze', 'Start', '13:00');
CALL add_p2p_check('Aboba', 'Cagoga', 'A1_Maze', 'Success', '13:30');
CALL add_p2p_check('Aboba', 'Cagoga', 'A2_SimpleNavigator_v1.0', 'Start', '14:00');
CALL add_p2p_check('Aboba', 'Cagoga', 'A2_SimpleNavigator_v1.0', 'Success', '14:30');
CALL add_p2p_check('Aboba', 'Derevo', 'A3_Parallels', 'Start', '15:00');
CALL add_p2p_check('Aboba', 'Derevo', 'A3_Parallels', 'Success', '15:30');

SELECT * FROM checks;

INSERT INTO xp("check", xp_amount)
VALUES (
        (SELECT MAX(id)
           FROM checks
          WHERE task LIKE 'A1_Maze'
            AND peer = 'Aboba'), 300);
INSERT INTO xp("check", xp_amount)
VALUES (
        (SELECT MAX(id)
           FROM checks
          WHERE task LIKE 'A2_SimpleNavigator_v1.0'
            AND peer = 'Aboba'), 400);
INSERT INTO xp("check", xp_amount)
VALUES (
        (SELECT MAX(id)
           FROM checks
          WHERE task LIKE 'A3_Parallels'
            AND peer = 'Aboba'), 300);

SELECT * FROM xp;

SELECT * FROM get_peers_completed_whole_block('A');

---------------------------- 3.8 ----------------------------------
-- Determine which peer each student should go to for a check.
-- You should determine it according to the recommendations of the peer's friends,
-- i.e. you need to find the peer with the greatest number of friends who recommend to be checked by him.
-- Output format: peer's nickname, nickname of the checker found

SELECT * FROM recommendations;
SELECT * FROM friends;

INSERT INTO peers
VALUES ('Fartlouv', '2001-03-08'),
       ('GigaChat', '1993-12-10'),
       ('Hermansberg', '1941-06-22'),
       ('Ingebordga', '1970-01-01'),
       ('Japaneese', '1994-05-06');

INSERT INTO friends(peer_1, peer_2)
VALUES ('Aboba', 'Fartlouv'),
       ('Baboba', 'GigaChat'),
       ('Aboba', 'GigaChat'),
       ('GigaChat', 'Hermansberg'),
       ('GigaChat', 'Derevo'),
       ('GigaChat', 'Egogo'),
       ('GigaChat', 'Ingebordga'),
       ('GigaChat', 'Japaneese'),
       ('Japaneese', 'Hermansberg'),
       ('Japaneese', 'Ingebordga'),
       ('Hermansberg', 'Derevo'),
       ('Hermansberg', 'Cagoga'),
       ('Hermansberg', 'Baboba'),
       ('Ingebordga', 'Baboba');

INSERT INTO recommendations(peer, recommended_peer)
VALUES ('Aboba', 'Fartlouv'),
       ('Baboba', 'GigaChat'),
       ('Aboba', 'GigaChat'),
       ('GigaChat', 'Hermansberg'),
       ('GigaChat', 'Derevo'),
       ('GigaChat', 'Egogo');


INSERT INTO recommendations(peer, recommended_peer)
VALUES ('Baboba', 'Aboba'),
       ('Baboba', 'Egogo'),
       ('Baboba', 'Derevo');

DROP FUNCTION IF EXISTS get_recommended_checkers();
CREATE OR REPLACE FUNCTION get_recommended_checkers()
RETURNS TABLE(peer VARCHAR, recommended_peer VARCHAR)
AS $$
BEGIN
    RETURN QUERY
      WITH friends_recommend AS (
    SELECT p.nickname,
           r.recommended_peer,
           COUNT(r.recommended_peer) AS quantity
      FROM peers AS p
           LEFT JOIN friends AS f
           ON f.peer_1 = p.nickname
           INNER JOIN recommendations AS r
           ON f.peer_2 = r.peer
           AND p.nickname != r.recommended_peer
     GROUP BY p.nickname,
              r.recommended_peer
    ),
           ranked_recomendations AS (
    SELECT fr.nickname,
           fr.recommended_peer,
           fr.quantity,
           DENSE_RANK() OVER(PARTITION BY
               fr.nickname
               ORDER BY fr.quantity DESC) AS rank
      FROM friends_recommend AS fr
    )
    SELECT rr.nickname,
           rr.recommended_peer
      FROM ranked_recomendations AS rr
     WHERE rank = 1;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_recommended_checkers();

---------------------------- 3.9 ----------------------------------
-- Determine the percentage of peers who:
--
-- Started only block 1
-- Started only block 2
-- Started both
-- Have not started any of them
--
-- A peer is considered to have started a block if he has at least one check of any task
-- from this block (according to the Checks table)
-- Procedure parameters: name of block 1, for example SQL, name of block 2, for example A.
-- Output format: percentage of those who started only the first block, percentage of those
-- who started only the second block, percentage of those who started both blocks, percentage
-- of those who did not started any of them

DROP FUNCTION IF EXISTS  calculate_block_participation(block1 VARCHAR, block2 VARCHAR);
CREATE OR REPLACE FUNCTION calculate_block_participation(block1 VARCHAR, block2 VARCHAR)
RETURNS TABLE (started_block1 FLOAT, started_block2 FLOAT, started_both_blocks FLOAT, didnt_start_any_block FLOAT)
AS $$
DECLARE peers_count INTEGER;
BEGIN
    SELECT COUNT(*)
      INTO peers_count
      FROM peers;

    RETURN QUERY
      WITH block1_participants AS (
    SELECT DISTINCT peer
      FROM checks AS c
     WHERE c.task ~ CONCAT(block1, '[0-9].*')
    ),
           block2_participants AS (
    SELECT DISTINCT peer
      FROM checks AS c
     WHERE c.task ~ CONCAT(block2, '[0-9].*')
    ),
           started_only_block1 AS (
    SELECT peer
      FROM block1_participants
    EXCEPT
    SELECT peer
      FROM block2_participants
    ),
           started_only_block2 AS (
    SELECT peer
      FROM block2_participants
    EXCEPT
    SELECT peer
      FROM block1_participants
    ),
           started_both_blocks AS (
    SELECT peer
      FROM block1_participants
    INTERSECT
    SELECT peer
      FROM block2_participants
    ),
           didnt_start_any_block AS (
    SELECT nickname AS peer
      FROM peers
     WHERE nickname NOT IN (
           SELECT *
             FROM block1_participants
            UNION ALL
           SELECT *
             FROM block2_participants
         )
    )
    SELECT (SELECT COUNT(*) FROM started_only_block1)::FLOAT / peers_count * 100,
           (SELECT COUNT(*) FROM started_only_block2)::FLOAT / peers_count * 100,
           (SELECT COUNT(*) FROM started_both_blocks)::FLOAT / peers_count * 100,
           (SELECT COUNT(*) FROM didnt_start_any_block)::FLOAT / peers_count * 100;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM calculate_block_participation('A', 'DO');

---------------------------- 3.10 ---------------------------------
-- Determine the percentage of peers who have ever successfully passed a check on their birthday
-- Also determine the percentage of peers who have ever failed a check on their birthday.
-- Output format: percentage  of peers who have ever successfully passed a check on their
-- birthday, percentage of peers who have ever failed a check on their birthday

DROP FUNCTION IF EXISTS calculate_birthday_check_percent();
CREATE OR REPLACE FUNCTION calculate_birthday_check_percent()
RETURNS TABLE (successful_checks FLOAT, unsuccessful_checks FLOAT)
AS $$
DECLARE
    total_peers INTEGER;
    successful_peers INTEGER;
    unsuccessful_peers INTEGER;
BEGIN
    SELECT COUNT(DISTINCT nickname)
      INTO total_peers
      FROM peers;

    SELECT COUNT(*)
      INTO successful_peers
      FROM peers AS p
           INNER JOIN checks AS c
           ON p.nickname = c.peer
           INNER JOIN xp
           ON c.id = xp."check"
     WHERE EXTRACT(DAY FROM p.birthday) = EXTRACT(DAY FROM c.date)
       AND EXTRACT(MONTH FROM p.birthday) = EXTRACT(MONTH FROM c.date)
     GROUP BY p.nickname;

    SELECT COUNT(DISTINCT nickname)
      INTO unsuccessful_peers
      FROM peers AS p
           INNER JOIN checks AS c
           ON p.nickname = c.peer
           INNER JOIN p2p
           ON c.id = p2p."check"
           LEFT JOIN verter AS v
           ON c.id = v."check"
     WHERE (p2p.state = 'Failure' OR v.state = 'Failure')
       AND EXTRACT(DAY FROM p.birthday) = EXTRACT(DAY FROM c.date)
       AND EXTRACT(MONTH FROM p.birthday) = EXTRACT(MONTH FROM c.date);

    RETURN QUERY
    SELECT
        (successful_peers::FLOAT / total_peers * 100),
        (unsuccessful_peers::FLOAT / total_peers * 100);
END;
$$ LANGUAGE plpgsql;

UPDATE peers
SET birthday = '2003-09-24'
WHERE nickname = 'Aboba';

UPDATE peers
SET birthday = '2000-09-25'
WHERE nickname = 'Baboba';

UPDATE peers
SET birthday = '1969-09-26'
WHERE nickname = 'Derevo';


SELECT * FROM calculate_birthday_check_percent();

---------------------------- 3.11 ---------------------------------
-- Determine all peers who did the given tasks 1 and 2, but did not do task 3
-- Procedure parameters: names of tasks 1, 2 and 3.
-- Output format: list of peers

DROP FUNCTION IF EXISTS  find_peers_not_completed_task(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR);
CREATE OR REPLACE FUNCTION find_peers_not_completed_task(task1 VARCHAR, task2 VARCHAR, task3 VARCHAR)
RETURNS TABLE (peer_name VARCHAR)
AS $$
BEGIN
    RETURN QUERY
      WITH completed_1 AS (
    SELECT DISTINCT peer
      FROM checks AS c
           INNER JOIN p2p
           ON c.id = p2p."check"
           LEFT JOIN verter AS v
           ON c.id = v."check"
     WHERE c.task = task1
       AND p2p.state = 'Success'
       AND (v.state = 'Success'
        OR v.state IS NULL)
    ),
           completed_2 AS (
    SELECT DISTINCT peer
      FROM checks AS c
           INNER JOIN p2p
           ON c.id = p2p."check"
           LEFT JOIN verter AS v
           ON c.id = v."check"
     WHERE c.task = task2
       AND p2p.state = 'Success'
       AND (v.state = 'Success'
        OR v.state IS NULL)
    ),
           not_completed_3 AS (
     SELECT DISTINCT peer
       FROM checks
      WHERE peer NOT IN (
            SELECT peer
              FROM checks AS c
                   INNER JOIN p2p
                   ON c.id = p2p."check"
                   LEFT JOIN verter AS v
                   ON c.id = v."check"
             WHERE c.task = task3
               AND p2p.state = 'Success'
               AND (v.state = 'Success'
                OR v.state IS NULL)
          )
    )
    SELECT *
       FROM completed_1
    INTERSECT
    SELECT *
      FROM completed_2
    INTERSECT
    SELECT *
      FROM not_completed_3;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM find_peers_not_completed_task('A1_Maze', 'A2_SimpleNavigator_v1.0', 'DO6_CICD');

---------------------------- 3.12 ---------------------------------
-- Using recursive common table expression, output the number of preceding tasks for each task
-- I. e. How many tasks have to be done, based on entry conditions, to get access to the current one.
-- Output format: task name, number of preceding tasks

DROP FUNCTION IF EXISTS get_task_hierarchy();
CREATE OR REPLACE FUNCTION get_task_hierarchy()
RETURNS TABLE (task VARCHAR, prev_count INTEGER)
AS $$
BEGIN
  RETURN QUERY
    WITH RECURSIVE task_hierarchy AS (
  SELECT title AS current_task,
         0 AS count_prev
    FROM tasks
   WHERE parent_task IS NULL
   UNION ALL
  SELECT t.title AS current_task,
         th.count_prev + 1 AS count_prev
    FROM tasks  AS t
         INNER JOIN task_hierarchy AS th
         ON t.parent_task = th.current_task
    )
 SELECT SPLIT_PART(current_task, '_', 1)::VARCHAR,
        count_prev
   FROM task_hierarchy;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM tasks;

SELECT * FROM get_task_hierarchy();

---------------------------- 3.13 ---------------------------------
-- Find "lucky" days for checks. A day is considered "lucky" if it has at least N consecutive successful checks
-- Parameters of the procedure: the N number of consecutive successful checks .
-- The time of the check is the start time of the P2P step.
-- Successful consecutive checks are the checks with no unsuccessful checks in between.
-- The amount of XP for each of these checks must be at least 80% of the maximum.
-- Output format: list of days

DROP FUNCTION IF EXISTS get_checks_lucky_days(n INTEGER);
CREATE OR REPLACE FUNCTION get_checks_lucky_days(n INTEGER)
RETURNS TABLE(lucky_day DATE)
AS $$
    WITH
    checks_starts AS (
    SELECT checks.id,
           tasks.max_xp,
           checks.date,
           p2p.time
      FROM checks
           INNER JOIN p2p ON checks.id = p2p."check"
           INNER JOIN tasks ON checks.task = tasks.title
     WHERE state = 'Start'
    ),
    checks_results AS (
    SELECT DISTINCT ON (id)
           id,
           state,
           date,
           time
      FROM (
            SELECT checks.id,
                   checks.date,
                   p2p.state,
                   p2p.time
              FROM checks
                   INNER JOIN p2p ON checks.id = p2p."check"
             UNION ALL
            SELECT checks.id,
                   checks.date,
                   v.state,
                   v.time
              FROM checks
                   INNER JOIN verter v ON checks.id = v."check"
       ) chk
     ORDER BY id, time DESC
    ),
    success_countings AS (
    SELECT cr.date, sum(CASE
                     WHEN cr.state = 'Success'
                          AND xp.xp_amount::float/cs.max_xp >= 0.8
                     THEN 1
                     ELSE 0
                     END) OVER sliding_checks AS sequential_successes
      FROM checks_starts cs
           INNER JOIN checks_results cr ON cs.id = cr.id AND cs."date" = cr."date"
           LEFT JOIN xp ON xp."check" = cr.id
    WINDOW sliding_checks AS (PARTITION BY cs."date" ORDER BY cs."time" ASC ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)
    )
    SELECT date AS lucky_day
      FROM success_countings
     GROUP BY date
    HAVING bool_or(sequential_successes >= n);
$$ LANGUAGE sql;

SELECT * FROM get_checks_lucky_days(1);

SELECT * FROM get_checks_lucky_days(2);


---------------------------- 3.14 ---------------------------------
-- Find the peer with the highest amount of XP
-- Output format: peer's nickname, amount of XP

DROP FUNCTION IF EXISTS get_peer_with_maximum_xp();
CREATE OR REPLACE FUNCTION get_peer_with_maximum_xp()
RETURNS TABLE(peer VARCHAR, xp INTEGER)
AS $$
BEGIN
    RETURN QUERY
      WITH xp_rating AS (
    SELECT c.peer AS nick,
           SUM(xp_amount)::INTEGER AS xp_amount,
           RANK() OVER (ORDER BY SUM(xp_amount) DESC) AS xp_rank
      FROM checks AS c
           INNER JOIN xp
           ON c.id = xp."check"
     GROUP BY nick
     )
    SELECT nick,
           xp_amount
      FROM xp_rating
     WHERE xp_rank = 1;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_peer_with_maximum_xp();

---------------------------- 3.15 ---------------------------------
-- Determine the peers that came before the given time at least N times during the whole time
-- Procedure parameters: time, N number of times .
-- Output format: list of peers

DROP FUNCTION IF EXISTS get_early_bird_peers();
CREATE OR REPLACE FUNCTION get_early_bird_peers(entry_time TIME, quantity INTEGER)
RETURNS TABLE(peer VARCHAR)
AS $$
BEGIN
    RETURN QUERY
    SELECT tt.peer
      FROM time_tracking AS tt
     WHERE tt.state = 1
       AND tt.time < entry_time
     GROUP BY tt.peer
    HAVING COUNT(*) >= quantity;
END;
$$ LANGUAGE plpgsql;

INSERT INTO time_tracking(peer, date, time, state)
VALUES ('GigaChat', '2023-09-01', '11:00:00', 1),
       ('GigaChat', '2023-09-01', '12:00:00', 2),
       ('GigaChat', '2023-09-01', '13:00:00', 1),
       ('GigaChat', '2023-09-01', '14:00:00', 2),
       ('GigaChat', '2023-09-01', '15:00:00', 1),
       ('Ingebordga', '2023-09-03', '09:00:00', 1),
       ('Ingebordga', '2023-09-03', '10:00:00', 2),
       ('Ingebordga', '2023-09-03', '11:00:00', 1),
       ('Ingebordga', '2023-09-03', '21:00:00', 2),
       ('Fartlouv', '2023-09-04', '11:00:00', 1),
       ('Fartlouv', '2023-09-04', '12:00:00', 2),
       ('Fartlouv', '2023-09-04', '15:00:00', 1),
       ('Fartlouv', '2023-09-04', '19:00:00', 2);

SELECT * FROM time_tracking;

SELECT * FROM get_early_bird_peers('15:00:00', 2);

SELECT * FROM get_early_bird_peers('12:00:00', 1);

SELECT * FROM get_early_bird_peers('17:00:00', 3);

---------------------------- 3.16 ---------------------------------
-- Determine the peers who left the campus more than M times during the last N days
-- Procedure parameters: N number of days , M number of times .
-- Output format: list of peers

DROP FUNCTION IF EXISTS peers_lefts_last_days();
CREATE OR REPLACE FUNCTION peers_lefts_last_days(days INTEGER, lefts INTEGER)
RETURNS TABLE(peer VARCHAR)
AS $$
BEGIN
    RETURN QUERY
      WITH last_left_in_day AS (
    SELECT tt.peer,
           tt.date,
           MAX(tt.time) AS last_time
      FROM time_tracking AS tt
     WHERE tt.date > CURRENT_DATE - days
     GROUP BY tt.peer,
              tt.date
    )
    SELECT tt.peer
      FROM time_tracking AS tt
           INNER JOIN last_left_in_day AS ll
           ON tt.peer = ll.peer
           AND tt.date = ll.date
           AND tt.time != ll.last_time
     WHERE tt.state = 2
     GROUP BY tt.peer
    HAVING COUNT(*) > lefts;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM time_tracking;

SELECT * FROM peers_lefts_last_days(30, 0);

SELECT * FROM peers_lefts_last_days(50, 0);

SELECT * FROM peers_lefts_last_days(40, 1);

---------------------------- 3.17 ---------------------------------
-- Determine for each month the percentage of early entries
-- For each month, count how many times people born in that month came to campus
-- during the whole time (we'll call this the total number of entries).
-- For each month, count the number of times people born in that month have come
-- to campus before 12:00 in all time (we'll call this the number of early entries).
-- For each month, count the percentage of early entries to campus relative to the total number of entries.
-- Output format: month, percentage of early entries

DROP FUNCTION IF EXISTS early_entries_by_month_of_birth();
CREATE OR REPLACE FUNCTION early_entries_by_month_of_birth()
RETURNS TABLE(month VARCHAR, early_entries DECIMAL)
AS $$
BEGIN
    RETURN QUERY
      WITH all_entries AS (
    SELECT tt.peer,
           tt.date,
           MIN(tt.time) AS day_entry
      FROM time_tracking AS tt
     GROUP BY tt.peer,
              tt.date
      )
    SELECT TO_CHAR(p.birthday, 'Month')::VARCHAR AS month,
           ROUND((COUNT(*) FILTER (WHERE EXTRACT(HOUR FROM al.day_entry) < 12)::DECIMAL / COUNT(*)) * 100, 2) AS early_entries
      FROM peers AS p
           INNER JOIN all_entries AS al
           ON p.nickname = al.peer
     GROUP BY month
     ORDER BY month;
END;
$$ LANGUAGE plpgsql;

INSERT INTO time_tracking(peer, date, time, state)
VALUES ('Hermansberg', '2023-10-01', '14:00:00', 1),
       ('Hermansberg', '2023-10-01', '15:00:00', 2),
       ('Japaneese', '2023-10-01', '14:00:00', 1),
       ('Japaneese', '2023-10-01', '15:00:00', 2),
       ('Japaneese', '2023-10-02', '14:00:00', 1),
       ('Japaneese', '2023-10-02', '15:00:00', 2),
       ('Japaneese', '2023-10-03', '11:00:00', 1),
       ('Japaneese', '2023-10-03', '15:00:00', 2),
       ('Japaneese', '2023-10-03', '16:00:00', 1),
       ('Japaneese', '2023-10-03', '17:00:00', 2);

SELECT * FROM peers;

SELECT * FROM time_tracking;

SELECT * FROM early_entries_by_month_of_birth();
