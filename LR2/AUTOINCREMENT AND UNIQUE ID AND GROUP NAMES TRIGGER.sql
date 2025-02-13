--CREATE SEQUENCE seq_group_id START WITH 1;
--CREATE SEQUENCE seq_student_id START WITH 1;

CREATE OR REPLACE TRIGGER trg_auto_group_id
BEFORE INSERT ON groups
FOR EACH ROW
BEGIN
    -- Если group_id не передан явно, присваиваем автоинкрементное значение
    IF :NEW.group_id IS NULL THEN
        :NEW.group_id := seq_group_id.NEXTVAL;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_auto_student_id
BEFORE INSERT ON students
FOR EACH ROW
BEGIN
    IF :NEW.student_id IS NULL THEN
   :NEW.student_id := seq_student_id.NEXTVAL;
    END IF;
END;
/


CREATE OR REPLACE TRIGGER trg_unique_group_id
BEFORE INSERT OR UPDATE ON groups
FOR EACH ROW
DECLARE
    v_count NUMBER;
BEGIN
    IF INSERTING THEN
        SELECT COUNT(*) INTO v_count FROM groups WHERE group_id = :NEW.group_id;
        
        IF v_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Ошибка: group_id должен быть уникальным');
        END IF;
    END IF;
    
    IF UPDATING THEN
        NULL; 
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_unique_student_id
BEFORE INSERT ON students
FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;  
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM students WHERE student_id = :NEW.student_id;
    
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Ошибка: student_id должен быть уникальным');
    END IF;
    COMMIT;  
END;
/

CREATE OR REPLACE TRIGGER trg_unique_group_name
AFTER INSERT OR UPDATE ON groups
FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;  
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count 
    FROM groups 
    WHERE LOWER(group_name) = LOWER(:NEW.group_name)
    AND group_id <> :NEW.group_id; 
    
    IF v_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'Ошибка: Название группы должно быть уникальным');
    END IF;
    COMMIT;  
END;
/

/*
SELECT * FROM GROUPS;
SELECT * FROM STUDENTS;
--INSERT INTO groups (group_name) VALUES ('english');
--INSERT INTO students (student_name, group_id) VALUES ('Sam', 1);
SELECT * FROM GROUPS;
SELECT * FROM STUDENTS;
INSERT INTO groups (group_name) VALUES ('spanish');
--INSERT INTO students (student_name, group_id) VALUES ('Leonor', 2);
SELECT * FROM GROUPS;
SELECT * FROM STUDENTS;
UPDATE groups SET group_id = 1 WHERE group_id = 2; -- Ошибка
SELECT * FROM GROUPS;
SELECT * FROM STUDENTS;*/

