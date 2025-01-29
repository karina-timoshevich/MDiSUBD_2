SET SERVEROUTPUT ON;

CREATE OR REPLACE FUNCTION generate_insert_command (input_id IN NUMBER)
RETURN VARCHAR2 IS
    insert_command VARCHAR2(400);
    val_to_insert NUMBER;
BEGIN
    BEGIN
        SELECT val INTO val_to_insert
        FROM MyTable
        WHERE id = input_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Строка с ID = ' || input_id || ' не найдена.');
            val_to_insert := NULL;
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Ошибка: ' || SQLERRM);
            val_to_insert := NULL;
    END;

    insert_command := 'INSERT INTO MyTable (id, val) VALUES (' || input_id || ', ' || NVL(TO_CHAR(val_to_insert), 'NULL') || ');';
    DBMS_OUTPUT.PUT_LINE(insert_command);
    
    RETURN insert_command;
END generate_insert_command;
/
SELECT generate_insert_command(835) FROM dual;
SELECT generate_insert_command(835555) FROM dual;
