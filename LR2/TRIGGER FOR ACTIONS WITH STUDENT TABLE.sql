CREATE OR REPLACE PACKAGE student_ctx AS
    TYPE t_group_name_table IS TABLE OF VARCHAR2(255) INDEX BY BINARY_INTEGER;
    g_group_names t_group_name_table;
    PROCEDURE load_group_name(p_group_id NUMBER, p_group_name VARCHAR2);
END student_ctx;
/

CREATE OR REPLACE PACKAGE BODY student_ctx AS
    PROCEDURE load_group_name(p_group_id NUMBER, p_group_name VARCHAR2) IS
    BEGIN
        g_group_names(p_group_id) := p_group_name;
    END load_group_name;
END student_ctx;
/

CREATE OR REPLACE TRIGGER cache_group_on_insert
AFTER INSERT OR UPDATE ON groups
FOR EACH ROW
BEGIN
    student_ctx.load_group_name(:NEW.group_id, :NEW.group_name);
END;
/

CREATE TABLE students_logs (
    LOG_ID NUMBER PRIMARY KEY,
    ACTION_TYPE VARCHAR2(10),
    OLD_ID NUMBER,
    NEW_ID NUMBER,
    OLD_NAME VARCHAR2(255),
    NEW_NAME VARCHAR2(255),
    OLD_GROUP_ID NUMBER,
    NEW_GROUP_ID NUMBER,
    OLD_GROUP_NAME VARCHAR2(255),
    NEW_GROUP_NAME VARCHAR2(255),
    ACTION_TIME TIMESTAMP
);
/

CREATE SEQUENCE STUDENTS_LOGS_SEQ START WITH 1;
/

CREATE OR REPLACE TRIGGER log_student_changes
AFTER INSERT OR UPDATE OR DELETE ON students
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO students_logs (LOG_ID, ACTION_TYPE, NEW_ID, NEW_NAME, NEW_GROUP_ID, NEW_GROUP_NAME, ACTION_TIME)
        VALUES (STUDENTS_LOGS_SEQ.NEXTVAL, 'INSERT', :NEW.student_id, :NEW.student_name, :NEW.group_id, student_ctx.g_group_names(:NEW.group_id), SYSTIMESTAMP);
    ELSIF UPDATING THEN
        INSERT INTO students_logs (LOG_ID, ACTION_TYPE, OLD_ID, NEW_ID, OLD_NAME, NEW_NAME, OLD_GROUP_ID, OLD_GROUP_NAME, NEW_GROUP_ID, NEW_GROUP_NAME, ACTION_TIME)
        VALUES (STUDENTS_LOGS_SEQ.NEXTVAL, 'UPDATE', :OLD.student_id, :NEW.student_id, :OLD.student_name, :NEW.student_name, :OLD.group_id, student_ctx.g_group_names(:OLD.group_id), :NEW.group_id, student_ctx.g_group_names(:NEW.group_id), SYSTIMESTAMP);
    ELSIF DELETING THEN
        INSERT INTO students_logs (LOG_ID, ACTION_TYPE, OLD_ID, OLD_NAME, OLD_GROUP_ID, OLD_GROUP_NAME, ACTION_TIME)
        VALUES (STUDENTS_LOGS_SEQ.NEXTVAL, 'DELETE', :OLD.student_id, :OLD.student_name, :OLD.group_id, student_ctx.g_group_names(:OLD.group_id), SYSTIMESTAMP);
    END IF;
END;
/

/*
INSERT INTO students (student_name, group_id) VALUES ('Charlie', 2);
SELECT * FROM students_logs;
UPDATE students SET group_id = 22 WHERE student_id = 49;
SELECT * FROM students_logs;
DELETE FROM students WHERE student_id = 50;
SELECT * FROM students_logs;*/

