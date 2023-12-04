DROP TABLE IF EXISTS peers CASCADE;
DROP TABLE IF EXISTS tasks CASCADE;
DROP TABLE IF EXISTS tasks CASCADE;
DROP TYPE IF EXISTS check_status CASCADE;
DROP TABLE IF EXISTS p2p CASCADE;
DROP TABLE IF EXISTS verter CASCADE;
DROP TABLE IF EXISTS checks CASCADE;
DROP TABLE IF EXISTS tasks CASCADE;
DROP TABLE IF EXISTS transferred_points CASCADE;
DROP TABLE IF EXISTS friends CASCADE;
DROP TABLE IF EXISTS recommendations CASCADE;
DROP TABLE IF EXISTS xp CASCADE;
DROP TABLE IF EXISTS time_tracking CASCADE;

CREATE TABLE IF NOT EXISTS peers (
    nickname VARCHAR UNIQUE PRIMARY KEY,
    birthday DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    title VARCHAR UNIQUE PRIMARY KEY DEFAULT NULL,
    parent_task VARCHAR DEFAULT NULL,
    max_xp INTEGER NOT NULL CHECK (max_xp > 0),
    FOREIGN KEY (parent_task) REFERENCES tasks(title)
);

CREATE OR REPLACE FUNCTION fnc_trg_check_single_root_task() RETURNS trigger
AS $$
    BEGIN
        IF (SELECT COUNT(*) FROM tasks WHERE parent_task IS NULL) > 1
        THEN
            RAISE EXCEPTION 'there can be only one root task (task without parent task)';
        END IF;
        RETURN NULL;
    END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_check_single_root_task
AFTER INSERT OR UPDATE OR DELETE
ON tasks
FOR EACH ROW EXECUTE FUNCTION fnc_trg_check_single_root_task();

CREATE TYPE check_status AS ENUM (
    'Start',
    'Success',
    'Failure'
);

CREATE TABLE IF NOT EXISTS checks (
    id SERIAL PRIMARY KEY,
    peer VARCHAR NOT NULL,
    task VARCHAR NOT NULL,
    "date" DATE NOT NULL,
    FOREIGN KEY (peer) REFERENCES peers(nickname),
    FOREIGN KEY (task) REFERENCES tasks(title)
);

CREATE TABLE IF NOT EXISTS p2p (
    id SERIAL PRIMARY KEY,
    "check" INTEGER NOT NULL,
    checking_peer VARCHAR NOT NULL,
    state check_status NOT NULL,
    "time" TIME,
    FOREIGN KEY ("check") REFERENCES checks(id),
    FOREIGN KEY (checking_peer) REFERENCES peers(nickname)
);

CREATE OR REPLACE FUNCTION fnc_inconsistent_checks(tbl varchar, check_id integer) RETURNS bool
AS $$
    DECLARE
        result bool;
    BEGIN
        EXECUTE FORMAT('SELECT EXISTS (
        SELECT COUNT(*)
          FROM %s
         WHERE "check" = %s
         GROUP BY "check"
        HAVING CASE
               WHEN SUM(CASE WHEN state = ''Start'' THEN 1 ELSE 0 END) = 0 THEN
                 CASE WHEN bool_or(state != ''Start'') THEN true ELSE false END
               WHEN SUM(CASE WHEN state = ''Start'' THEN 1 ELSE 0 END) > 1 THEN true
               ELSE
                 SUM(CASE WHEN state != ''Start'' THEN 1 ELSE 0 END) > 1
               END
        )', tbl, check_id)
        INTO result;
        RETURN result;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_trg_finish_after_start() RETURNS trigger
AS $$
    DECLARE
        check_id INTEGER;
    BEGIN
       IF TG_NARGS != 1 THEN
         RAISE EXCEPTION 'there must be one argument in trigger function';
       END IF;

       IF TG_OP IN ('INSERT', 'UPDATE') THEN
           check_id := NEW."check";
       ELSIF TG_OP = 'DELETE' THEN
           check_id := OLD."check";
       END IF;

       IF fnc_inconsistent_checks(tg_argv[0], check_id)
       THEN
           RAISE EXCEPTION '(un)successful check must be preceded by check with status "Started" and there must be one start check';
       END IF;
       RETURN NULL;
    END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_finish_after_start
AFTER INSERT OR UPDATE OR DELETE
ON p2p
FOR EACH ROW EXECUTE FUNCTION fnc_trg_finish_after_start('p2p');

CREATE UNIQUE INDEX unique_peer_for_check
ON p2p("check", checking_peer)
WHERE state = 'Start';

CREATE TABLE IF NOT EXISTS verter (
    id SERIAL PRIMARY KEY,
    "check" INTEGER NOT NULL,
    state check_status NOT NULL,
    "time" TIME NOT NULL,
    FOREIGN KEY ("check") REFERENCES checks(id)
);

CREATE OR REPLACE FUNCTION fnc_trg_verter_after_p2p() RETURNS trigger
AS $$
    DECLARE
        check_id integer;
    BEGIN
       IF TG_OP IN ('INSERT', 'UPDATE') THEN
           check_id := NEW."check";
       ELSIF TG_OP = 'DELETE' THEN
           check_id := OLD."check";
       END IF;

       IF EXISTS (
           SELECT COUNT(*)
             FROM verter
                  LEFT JOIN p2p p on verter."check" = p."check"
            WHERE verter."check" = check_id
            GROUP BY verter."check"
           HAVING NOT bool_or(p.state = 'Success')
                  OR max(p.time) > min(verter.time)
           )
       THEN
          RAISE EXCEPTION 'verter check must be preceded by a successful P2P check';
       END IF;
       RETURN NULL;
    END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER trg_verter_after_p2p
AFTER INSERT OR UPDATE OR DELETE
ON verter
FOR EACH ROW EXECUTE FUNCTION fnc_trg_verter_after_p2p();

CREATE CONSTRAINT TRIGGER trg_finish_after_start
AFTER INSERT OR UPDATE OR DELETE
ON verter
FOR EACH ROW EXECUTE FUNCTION fnc_trg_finish_after_start('verter');

CREATE TABLE IF NOT EXISTS transferred_points (
    id SERIAL PRIMARY KEY,
    checking_peer VARCHAR NOT NULL,
    checked_peer VARCHAR NOT NULL CHECK (checking_peer != checked_peer),
    points_amount INTEGER,
    FOREIGN KEY (checking_peer) REFERENCES peers(nickname),
    FOREIGN KEY (checked_peer) REFERENCES peers(nickname)
);

CREATE TABLE IF NOT EXISTS friends (
    id SERIAL PRIMARY KEY,
    peer_1 VARCHAR NOT NULL,
    peer_2 VARCHAR NOT NULL,
    FOREIGN KEY (peer_1) REFERENCES peers(nickname),
    FOREIGN KEY (peer_2) REFERENCES peers(nickname),
    UNIQUE(peer_1, peer_2)
);

CREATE OR REPLACE FUNCTION fnc_trg_mutual_friendship() RETURNS trigger
AS $$
    BEGIN
       INSERT INTO friends(peer_1, peer_2)
              VALUES (NEW.peer_2, NEW.peer_1);
       RETURN NULL;
    END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_mutual_friendship ON friends;

CREATE TRIGGER trg_mutual_friendship
AFTER INSERT
ON friends
FOR EACH ROW 
WHEN (pg_trigger_depth() < 1)
EXECUTE FUNCTION fnc_trg_mutual_friendship();

CREATE TABLE IF NOT EXISTS recommendations (
    id SERIAL PRIMARY KEY,
    peer VARCHAR NOT NULL,
    recommended_peer VARCHAR NOT NULL,
    FOREIGN KEY (peer) REFERENCES peers(nickname),
    FOREIGN KEY (recommended_peer) REFERENCES peers(nickname)
);

CREATE TABLE IF NOT EXISTS xp (
    id SERIAL PRIMARY KEY,
    "check" INTEGER NOT NULL,
    xp_amount INTEGER NOT NULL,
    FOREIGN KEY ("check") REFERENCES checks(id)
);

CREATE TABLE IF NOT EXISTS time_tracking
(
    id     SERIAL PRIMARY KEY,
    peer   VARCHAR NOT NULL,
    "date" DATE    NOT NULL,
    "time" TIME    NOT NULL,
    state  INTEGER CHECK (state IN (1, 2)),
    FOREIGN KEY (peer) REFERENCES peers(nickname)
);

-- no trigger for equality of entries and leaves because this is stupid

CREATE OR REPLACE PROCEDURE import_from_csv(tbl varchar, filename varchar, columns varchar DEFAULT '')
AS $$
    BEGIN
        EXECUTE FORMAT('COPY %s%s FROM ''%s'' (FORMAT csv, DELIMITER '','', HEADER true)', tbl, columns, filename);
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE export_to_csv(tbl varchar, filename varchar, columns varchar DEFAULT '')
AS $$
    BEGIN
        EXECUTE FORMAT('COPY %s%s TO ''%s'' (FORMAT csv, DELIMITER '','', HEADER true)', tbl, columns, filename);
    end;
$$ LANGUAGE plpgsql;

DO $$
    DECLARE
        dirpath varchar := '/Users/wilmerno/SQL2_Info21_v1.0-1/src/';
    BEGIN
        CALL import_from_csv('peers', dirpath || './csvdata/peers.csv');
        CALL import_from_csv('tasks', dirpath || './csvdata/tasks.csv');
        CALL import_from_csv('checks', dirpath || './csvdata/checks.csv', '(peer,task,date)');
        CALL import_from_csv('p2p', dirpath || './csvdata/p2p.csv', '("check",checking_peer,state,time)');
        CALL import_from_csv('verter', dirpath || './csvdata/verter.csv', '("check",state,time)');
        CALL import_from_csv('transferred_points', dirpath || './csvdata/transferred_points.csv',
    '(checking_peer,checked_peer,points_amount)');
        CALL import_from_csv('friends', dirpath || './csvdata/friends.csv', '(peer_1,peer_2)');
        CALL import_from_csv('recommendations', dirpath || './csvdata/recommendations.csv', '(peer,recommended_peer)');
        CALL import_from_csv('xp', dirpath || './csvdata/xp.csv', '("check",xp_amount)');
        CALL import_from_csv('time_tracking', dirpath || './csvdata/time_tracking.csv', '(peer,"date","time",state)');
    END;
$$ LANGUAGE plpgsql;


