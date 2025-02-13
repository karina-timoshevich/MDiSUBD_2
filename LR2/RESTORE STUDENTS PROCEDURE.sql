CREATE OR REPLACE PROCEDURE restore_students_from_logs(
    p_time TIMESTAMP DEFAULT NULL,
    p_offset INTERVAL DAY TO SECOND DEFAULT NULL
) IS
    v_restore_time TIMESTAMP;
    v_group_exists NUMBER;
    v_student_exists NUMBER;
    v_count_deleted NUMBER := 0;
BEGIN
    IF p_time IS NOT NULL THEN
        v_restore_time := p_time;
    ELSIF p_offset IS NOT NULL THEN
        v_restore_time := SYSTIMESTAMP - p_offset;
    ELSE
        RAISE_APPLICATION_ERROR(-20000, 'Нужно передать либо p_time, либо p_offset.');
    END IF;

    DBMS_OUTPUT.PUT_LINE('Восстанавливаем данные с ' || TO_CHAR(v_restore_time, 'DD-MM-YYYY HH24:MI:SS'));

    SELECT COUNT(*) INTO v_count_deleted
    FROM students_logs
    WHERE action_time >= v_restore_time
      AND action_type = 'DELETE';

    IF v_count_deleted = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Нет записей DELETE в students_logs. Восстановление не требуется.');
        RETURN;
    END IF;

    FOR record IN (
        SELECT DISTINCT old_group_id, old_group_name
        FROM students_logs
        WHERE action_time >= v_restore_time
          AND old_group_id IS NOT NULL
          AND old_group_name IS NOT NULL
    ) LOOP
        SELECT COUNT(*) INTO v_group_exists
        FROM groups
        WHERE group_id = record.old_group_id;

        IF v_group_exists = 0 THEN
            INSERT INTO groups (group_id, group_name)
            VALUES (record.old_group_id, record.old_group_name);
            DBMS_OUTPUT.PUT_LINE('Восстановлена группа: ' || record.old_group_id || ' - ' || record.old_group_name);
        END IF;
    END LOOP;

    FOR record IN (
        SELECT * FROM students_logs
        WHERE action_time >= v_restore_time
          AND action_type = 'DELETE'
        ORDER BY action_time DESC
    ) LOOP
        SELECT COUNT(*) INTO v_student_exists
        FROM students
        WHERE student_id = record.old_id;

        IF v_student_exists = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TRIGGER trg_check_group_exists DISABLE';
            INSERT INTO students (student_id, student_name, group_id)
            VALUES (record.old_id, record.old_name, record.old_group_id);
            DBMS_OUTPUT.PUT_LINE('Восстановлен студент: ' || record.old_id || ' - ' || record.old_name);
            EXECUTE IMMEDIATE 'ALTER TRIGGER trg_check_group_exists ENABLE';
        ELSE
            DBMS_OUTPUT.PUT_LINE('Студент ' || record.old_id || ' уже существует, пропускаем.');
        END IF;
    END LOOP;

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Восстановление завершено.');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
        ROLLBACK;
END;
/


SELECT * FROM students;
SELECT * FROM groups;
/*DELETE FROM students WHERE student_id = 102;
DELETE FROM groups WHERE group_id = 41;
SELECT * FROM groups WHERE group_id =41;
SELECT * FROM students;
SELECT * FROM groups;
SELECT * FROM groups WHERE group_id =41;*/
SET SERVEROUTPUT ON;
/*BEGIN
    restore_students_from_logs(NULL, INTERVAL '1' MINUTE);
END;
/
*/
SELECT * FROM groups WHERE group_id =222;
SELECT * 
FROM students_logs 
WHERE action_time >= TIMESTAMP '2025-02-13 23:28:26' 
  AND action_type = 'DELETE';

BEGIN
    restore_students_from_logs(TIMESTAMP '2025-02-13 23:28:26', NULL);
END;
/
BEGIN
    restore_students_from_logs(NULL, INTERVAL '1' MINUTE);
END;
/
SELECT * FROM groups WHERE group_id =222;
SELECT * FROM students;
SELECT * FROM groups;

SHOW ERRORS PROCEDURE restore_students_from_logs;
