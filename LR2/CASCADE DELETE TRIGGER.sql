CREATE OR REPLACE PACKAGE global_variables AS
    is_group_delete_cascade BOOLEAN := FALSE;
END global_variables;
/

CREATE OR REPLACE TRIGGER trg_delete_group_cascade
BEFORE DELETE ON groups
FOR EACH ROW
BEGIN
    global_variables.is_group_delete_cascade := TRUE;
    
    DELETE FROM students
    WHERE group_id = :OLD.group_id;  

    global_variables.is_group_delete_cascade := FALSE;
EXCEPTION
    WHEN OTHERS THEN
        global_variables.is_group_delete_cascade := FALSE;
        RAISE;
END;
/

CREATE OR REPLACE TRIGGER trg_check_group_exists
BEFORE INSERT OR UPDATE ON students
FOR EACH ROW
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count 
    FROM groups 
    WHERE group_id = :NEW.group_id;  

    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Ошибка: Группа с ID ' || :NEW.group_id || ' не существует.');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER prevent_group_id_update
BEFORE UPDATE OF group_id ON groups
FOR EACH ROW
DECLARE
    students_exist NUMBER;
BEGIN
    SELECT COUNT(*) INTO students_exist
    FROM students
    WHERE group_id = :OLD.group_id;

    IF students_exist > 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Ошибка: У этой группы есть студенты. Изменение group_id запрещено.');
    END IF;
END;
/


SELECT * FROM GROUPS;
SELECT * FROM STUDENTS;
DELETE FROM groups WHERE group_id = 1;

SELECT * FROM GROUPS;
SELECT * FROM STUDENTS;
-- несуществующая группа 99
--INSERT INTO students (student_name, group_id) VALUES ('David', 99);

--INSERT INTO groups (group_name) VALUES ('Physics');
INSERT INTO students (student_name, group_id) VALUES ('Eve', 22);
SELECT * FROM students WHERE group_id = 22;
UPDATE groups SET group_id = 10 WHERE group_id = 22;

--INSERT INTO groups (group_name) VALUES ('english');
--INSERT INTO students (student_name, group_id) VALUES ('Jack', 1);
--INSERT INTO students (student_name, group_id) VALUES ('Lana', 1);