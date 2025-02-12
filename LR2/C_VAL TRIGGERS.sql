CREATE OR REPLACE TRIGGER trg_update_c_val_on_insert
BEFORE INSERT ON students
FOR EACH ROW
BEGIN
    UPDATE groups
    SET c_val = c_val + 1
    WHERE group_id = :NEW.group_id;
END;
/

CREATE OR REPLACE TRIGGER trg_update_c_val_on_delete
BEFORE DELETE ON students
FOR EACH ROW
BEGIN
    IF NOT global_variables.is_group_delete_cascade THEN
        UPDATE groups
        SET c_val = c_val - 1
        WHERE group_id = :OLD.group_id;
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_update_c_val_on_update
BEFORE UPDATE OF group_id ON students
FOR EACH ROW
BEGIN
    IF :OLD.group_id != :NEW.group_id THEN
        UPDATE groups
        SET c_val = c_val - 1
        WHERE group_id = :OLD.group_id;
        UPDATE groups
        SET c_val = c_val + 1
        WHERE group_id = :NEW.group_id;
    END IF;
END;
/


SELECT USER FROM dual;
SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual;

SELECT * FROM students;
SELECT * FROM groups;  
INSERT INTO students (student_name, group_id) VALUES ('Tolic', 22);
INSERT INTO students (student_name, group_id) VALUES ('Pasha', 22);
SELECT * FROM students;
SELECT * FROM groups;
UPDATE students SET group_id = 41 WHERE student_id = 48;
SELECT * FROM students;
SELECT * FROM groups;  

DELETE FROM students WHERE student_id = 52;
SELECT * FROM students;
SELECT * FROM groups;  
